// This file contains the handlers for the requests that the API Gateway receives from the client
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Jorrit05/DYNAMOS/pkg/api"
	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

type providerResponse struct {
	provider string
	body     string
	err      error
}

type providerErrorResponse struct {
	Provider string `json:"provider"`
	Error    string `json:"error"`
}

func requestHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("Starting requestApprovalHandler")
		requestTimeout := api.RequestTimeoutFromEnv(r, "API_GATEWAY_HTTP_TIMEOUT", "API_GATEWAY_HTTP_STREAM_TIMEOUT", 30*time.Second, 20*time.Minute)
		ctxWithTimeout, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()

		// Start a new span with the context that has a timeout
		ctx, span := trace.StartSpan(ctxWithTimeout, "requestApprovalHandler")
		defer span.End()

		body, err := api.GetRequestBody(w, r, serviceName)
		if err != nil {
			return
		}

		var apiReqApproval api.RequestApproval
		if err := json.Unmarshal(body, &apiReqApproval); err != nil {
			logger.Sugar().Errorf("Error unmMarshalling get apiReqApproval: %v", err)
			return
		}

		userPb := &pb.User{
			Id:       apiReqApproval.User.Id,
			UserName: apiReqApproval.User.UserName,
		}

		var dataRequestInterface map[string]interface{}
		if err := json.Unmarshal(apiReqApproval.DataRequest, &dataRequestInterface); err != nil {
			logger.Sugar().Errorf("Error unmarhsalling get request: %v", err)
			return
		}

		dataRequestOptions := &api.DataRequestOptions{}
		dataRequestOptions.Options = make(map[string]bool)
		if err := json.Unmarshal(apiReqApproval.DataRequest, &dataRequestOptions); err != nil {
			logger.Sugar().Errorf("Error unmMarshalling get apiReqApproval: %v", err)
			return
		}

		transport := ""
		if apiReqApproval.Transport != "" {
			transport = lib.NormalizeTransport(apiReqApproval.Transport)
		} else if dataRequestOptions.Transport != "" {
			transport = lib.NormalizeTransport(dataRequestOptions.Transport)
		}
		logger.Sugar().Infow("Resolved request transport",
			"topLevelTransport", apiReqApproval.Transport,
			"dataRequestTransport", dataRequestOptions.Transport,
			"normalizedTransport", transport,
		)
		delete(dataRequestInterface, "transport")

		dataRequestInterface["user"] = userPb

		// Create protobuf struct for the req approval flow
		protoRequest := &pb.RequestApproval{
			Type:             apiReqApproval.Type,
			User:             userPb,
			DataProviders:    apiReqApproval.DataProviders,
			DestinationQueue: "policyEnforcer-in",
			Options:          dataRequestOptions.Options,
			Transport:        transport,
		}

		// Create a channel to receive the response
		responseChan := make(chan validation)

		requestApprovalMutex.Lock()
		requestApprovalMap[protoRequest.User.Id] = responseChan
		requestApprovalMutex.Unlock()

		_, err = c.SendRequestApproval(ctx, protoRequest)
		if err != nil {
			logger.Sugar().Errorf("error in sending requestapproval: %v", err)
		}

		select {
		case validationStruct := <-responseChan:
			msg := validationStruct.response

			logger.Sugar().Infof("Received response, %s", msg.Type)
			logger.Sugar().Infow("Received request approval response transport",
				"responseTransport", msg.GetRequestMetadata().GetTransport(),
				"requestedTransport", transport,
			)
			if msg.Type != "requestApprovalResponse" {
				logger.Sugar().Errorf("Unexpected message received, type: %s", msg.Type)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}

			// Add necessary information for the data request in the request metadata
			requestMetadata := &pb.RequestMetadata{
				// Add the job id from the request approval to the data request body
				JobId: msg.JobId,
				// Keep the selected internal transport on the request path.
				Transport: transport,
				// initialize the map to add values to it
				Traces: make(map[string][]byte),
			}
			// Add the binary trace of the span to the data request (used for appending the traces)
			requestMetadata.Traces["binaryTrace"] = propagation.Binary(span.SpanContext())
			if msg.RequestMetadata != nil && msg.RequestMetadata.Transport != "" {
				requestMetadata.Transport = lib.NormalizeTransport(msg.RequestMetadata.Transport)
			} else {
				requestMetadata.Transport = lib.NormalizeTransport(transport)
			}
			// Set the data request interface to the request metadata from the previous steps
			dataRequestInterface["requestMetadata"] = requestMetadata

			// Marshal the combined data back into JSON for forwarding
			dataRequestJson, err := json.Marshal(dataRequestInterface)
			if err != nil {
				logger.Sugar().Errorf("Error marshalling combined data: %v", err)
				return
			}

			logger.Sugar().Infof("Data Prepared jsonData: %s", dataRequestJson)

			if streamed, err := streamDataToAuthProviders(ctx, w, r, dataRequestJson, msg.AuthorizedProviders, apiReqApproval.Type, msg.JobId); err != nil {
				logger.Sugar().Errorf("Error streaming data to providers: %v", err)
				if !streamed {
					http.Error(w, "Internal server error", http.StatusInternalServerError)
				}
				return
			} else if streamed {
				return
			}

			// Send the data to the authorized providers
			responses, hasProviderErrors := sendDataToAuthProviders(ctx, dataRequestJson, msg.AuthorizedProviders, apiReqApproval.Type, msg.JobId)
			if hasProviderErrors {
				w.WriteHeader(http.StatusBadGateway)
			} else {
				w.WriteHeader(http.StatusOK)
			}
			w.Write(responses)
			return

		case <-ctx.Done():
			http.Error(w, "Request timed out", http.StatusRequestTimeout)
			return
		}
	}
}

// Use the data request that was previously built and send it to the authorised providers
// acquired from the request approval
func sendDataToAuthProviders(ctx context.Context, dataRequest []byte, authorizedProviders map[string]string, msgType string, jobId string) ([]byte, bool) {
	var wg sync.WaitGroup
	results := make(chan providerResponse, len(authorizedProviders))

	// This will be replaced with AMQ in the future
	agentPort := "8080"
	// Iterate over each auth provider
	for auth, url := range authorizedProviders {
		wg.Add(1)
		target := strings.ToLower(auth)
		// Construct the end point
		endpoint := fmt.Sprintf("http://%s:%s/agent/v1/%s/%s", url, agentPort, msgType, target)

		// Print the request to the console (without \n to avoid the log only showing first line when searching)
		logger.Sugar().Infof("Sending request to %s. Endpoint: %s JSON:%v", target, endpoint, string(dataRequest))

		// Async call send the data
		go func(provider string, providerEndpoint string) {
			defer wg.Done()
			respData, err := sendDataWithHeaders(ctx, providerEndpoint, dataRequest, map[string]string{
				"Authorization": "bearer 1234",
			})
			results <- providerResponse{provider: provider, body: respData, err: err}
		}(auth, endpoint)
	}

	// Wait until all the requests are complete
	wg.Wait()
	close(results)
	logger.Sugar().Debug("Returning responses")

	responses := make([]string, 0, len(authorizedProviders))
	providerErrors := make([]providerErrorResponse, 0)
	for result := range results {
		if result.err != nil {
			logger.Sugar().Errorf("Error sending data to %s, %v", result.provider, result.err)
			providerErrors = append(providerErrors, providerErrorResponse{
				Provider: result.provider,
				Error:    result.err.Error(),
			})
			continue
		}
		responses = append(responses, result.body)
	}

	responseMap := map[string]interface{}{
		"jobId":     jobId,
		"responses": responses,
	}
	if len(providerErrors) > 0 {
		responseMap["providerErrors"] = providerErrors
	}

	// jsonResponse, _ := json.Marshal(responseMap)
	// return jsonResponse
	return cleanupAndMarshalResponse(responseMap), len(providerErrors) > 0
}

// Now assumes input is map[string]interface{} and directly marshals it to prettified JSON.
func cleanupAndMarshalResponse(responseMap map[string]interface{}) []byte {
	prettifiedJSON, err := json.MarshalIndent(responseMap, "", "    ")
	if err != nil {
		logger.Sugar().Errorf("Error marshalling cleaned response: %v", err)
	}
	return prettifiedJSON
}

func sendData(endpoint string, jsonData []byte) (string, error) {
	return sendDataWithHeaders(context.Background(), endpoint, jsonData, map[string]string{
		"Authorization": "bearer 1234",
	})
}

func sendDataWithHeaders(ctx context.Context, endpoint string, jsonData []byte, headers map[string]string) (string, error) {
	// FIXME: Change to an actual token in the future?
	// Request the data using the endpoint, body and headers
	body, err := api.PostRequestWithContext(ctx, endpoint, string(jsonData), headers)
	if err != nil {
		return "", err
	}

	// Print body (only use for debugging and testing, this is sometimes a very large output in the logs)
	// logger.Sugar().Debugf("Body: %v", body)

	// Here we should send the request over the socket
	// For now we should append it to a list so that we gather all responses and send them in bulk
	return string(body), nil
}

func streamDataToAuthProviders(ctx context.Context, w http.ResponseWriter, r *http.Request, dataRequest []byte, authorizedProviders map[string]string, msgType string, jobId string) (bool, error) {
	if !api.WantsNDJSON(r) {
		return false, nil
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		return false, nil
	}

	w.Header().Set("Content-Type", api.NDJSONContentType)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := api.WriteNDJSON(w, api.StreamResponse{
		Type:          api.StreamEventTypeJob,
		JobID:         jobId,
		ProviderCount: len(authorizedProviders),
	}); err != nil {
		return true, err
	}
	flusher.Flush()

	bufferSize := len(authorizedProviders) * 8
	if bufferSize < 16 {
		bufferSize = 16
	}
	updates := make(chan api.StreamResponse, bufferSize)
	var wg sync.WaitGroup

	agentPort := "8080"
	for auth, url := range authorizedProviders {
		provider := auth
		target := strings.ToLower(auth)
		endpoint := fmt.Sprintf("http://%s:%s/agent/v1/%s/%s", url, agentPort, msgType, target)
		logger.Sugar().Infof("Streaming request to %s. Endpoint: %s JSON:%v", target, endpoint, string(dataRequest))

		wg.Add(1)
		go func(providerName string, providerEndpoint string) {
			defer wg.Done()
			headers := map[string]string{
				"Authorization": "bearer 1234",
				"Accept":        api.NDJSONContentType,
			}
			err := api.PostRequestStream(ctx, providerEndpoint, string(dataRequest), headers, func(message []byte) error {
				select {
				case updates <- normalizeProviderStreamEvent(providerName, jobId, message):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if err != nil {
				select {
				case updates <- api.StreamResponse{
					Type:     api.StreamEventTypeProviderError,
					JobID:    jobId,
					Provider: providerName,
					Error:    err.Error(),
				}:
				case <-ctx.Done():
				}
			}
		}(provider, endpoint)
	}

	go func() {
		wg.Wait()
		close(updates)
	}()

	completedProviders := map[string]struct{}{}
	for {
		select {
		case update, ok := <-updates:
			if !ok {
				doneEvent := api.StreamResponse{
					Type:               api.StreamEventTypeDone,
					JobID:              jobId,
					ProviderCount:      len(authorizedProviders),
					CompletedProviders: len(completedProviders),
				}
				if err := api.WriteNDJSON(w, doneEvent); err != nil {
					return true, err
				}
				flusher.Flush()
				return true, nil
			}

			if update.Provider != "" && (update.Type == api.StreamEventTypeProviderError || (update.Type == api.StreamEventTypeProviderResult && !update.Partial)) {
				completedProviders[update.Provider] = struct{}{}
			}

			if err := api.WriteNDJSON(w, update); err != nil {
				return true, err
			}
			flusher.Flush()
		case <-ctx.Done():
			for providerName := range authorizedProviders {
				if _, completed := completedProviders[providerName]; completed {
					continue
				}
				if err := api.WriteNDJSON(w, api.StreamResponse{
					Type:     api.StreamEventTypeProviderError,
					JobID:    jobId,
					Provider: providerName,
					Error:    "request timed out",
				}); err != nil {
					return true, err
				}
				completedProviders[providerName] = struct{}{}
			}
			if err := api.WriteNDJSON(w, api.StreamResponse{
				Type:               api.StreamEventTypeDone,
				JobID:              jobId,
				ProviderCount:      len(authorizedProviders),
				CompletedProviders: len(completedProviders),
			}); err != nil {
				return true, err
			}
			flusher.Flush()
			return true, nil
		}
	}
}

func normalizeProviderStreamEvent(provider string, jobId string, payload []byte) api.StreamResponse {
	var event api.StreamResponse
	if err := json.Unmarshal(payload, &event); err == nil && event.Type != "" {
		if event.Provider == "" {
			event.Provider = provider
		}
		if event.JobID == "" {
			event.JobID = jobId
		}
		return event
	}

	event = api.StreamResponse{
		Type:     api.StreamEventTypeProviderResult,
		JobID:    jobId,
		Provider: provider,
	}
	event.SetResultBody(payload)
	return event
}

func availableProvidersHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("Starting requestApprovalHandler")
		var availableProviders = make(map[string]lib.AgentDetails)
		resp, err := getAvailableProviders()
		if err != nil {
			logger.Sugar().Errorf("Error getting available providers: %v", err)
			return
		}

		// Bind resp to availableProviders
		availableProviders = resp

		jsonResponse, err := json.Marshal(availableProviders)
		if err != nil {
			logger.Sugar().Errorf("Error marshalling result, %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(jsonResponse)
	}
}

// Maybe this should be moved into the orchestrarot
func getAvailableProviders() (map[string]lib.AgentDetails, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get the value from etcd.
	resp, err := etcdClient.Get(ctx, "/agents/online", clientv3.WithPrefix())
	if err != nil {
		logger.Sugar().Errorf("failed to get value from etcd: %v", err)
		return nil, err
	}

	// Initialize an empty map to store the unmarshaled structs.
	result := make(map[string]lib.AgentDetails)
	// Iterate through the key-value pairs and unmarshal the values into structs.
	for _, kv := range resp.Kvs {
		var target lib.AgentDetails
		err = json.Unmarshal(kv.Value, &target)
		if err != nil {
			// return nil, fmt.Errorf("failed to unmarshal JSON for key %s: %v", key, err)
		}
		result[string(target.Name)] = target
	}

	return result, nil

}
