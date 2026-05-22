// To aggregate two structpb.Struct instances by merging all identical columns, you would first need to iterate over the fields of both structs, compare them, and merge the identical ones. Here’s a simplified approach to how you might do it:

// Iterate through the fields of both data structures.
// For each field, check if the field exists in both structs.
// If a field exists in both, merge the values. This step depends on the nature of the data. For simplicity, let's assume we're just appending the values if they are lists or replacing them if they are singular values.
// If a field exists in only one of the structs, just carry it over to the resulting struct.
// This approach assumes that the data in identical columns can be meaningfully merged. For scalar fields (e.g., strings, numbers), you may need to decide whether to take one value over the other or to merge them based on specific logic.

// Here’s an example function that performs the merge:

package main

import (
	"context"
	"strconv"
	"sync"

	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

type aggregateStreamState struct {
	providerFinals map[string]struct{}
	seenBatches    map[string]struct{}
	nextSequence   int
	rowsProcessed  int
	rowsTotal      int
	buffered       []*pb.MicroserviceCommunication
}

var (
	aggregateStateMu sync.Mutex
	aggregateStates  = map[string]*aggregateStreamState{}
)

// This is the function being called by the first compute-side microservice when aggregation is enabled.
func handleSqlDataRequest(ctx context.Context, msComm *pb.MicroserviceCommunication) (*pb.MicroserviceCommunication, bool, bool, error) {
	ctx, span := trace.StartSpan(ctx, serviceName+"/handleSqlDataRequest")
	defer span.End()
	logger.Sugar().Infof("Start %s handleSqlDataRequest", serviceName)

	if msComm == nil {
		return nil, false, true, nil
	}

	sqlDataRequest := &pb.SqlDataRequest{}
	if err := msComm.OriginalRequest.UnmarshalTo(sqlDataRequest); err != nil {
		return nil, false, true, err
	}

	correlationID := aggregateCorrelationID(msComm)
	final := lib.MetadataBool(msComm.Metadata, lib.StreamFinalMetadataKey, true)
	expectedProviders := NR_OF_DATA_PROVIDERS
	if expectedProviders <= 0 {
		expectedProviders = 1
	}

	if sqlDataRequest.Algorithm == "average" {
		if sqlDataRequest.Options[lib.ClassicUnaryOptionKey] {
			merged, shouldForward, shouldStop := updateBufferedAggregateState(msComm, correlationID, expectedProviders, final)
			addTrace(merged, span)
			return merged, shouldForward, shouldStop, nil
		}

		forwarded, shouldForward, shouldStop := updateAverageStreamState(msComm, correlationID, expectedProviders, final)
		if !shouldForward {
			return nil, false, shouldStop, nil
		}
		addTrace(forwarded, span)
		return forwarded, shouldForward, shouldStop, nil
	}

	if sqlDataRequest.Options[lib.ClassicUnaryOptionKey] {
		merged, shouldForward, shouldStop := updateBufferedAggregateState(msComm, correlationID, expectedProviders, final)
		addTrace(merged, span)
		return merged, shouldForward, shouldStop, nil
	}

	forwarded, shouldForward, shouldStop := updatePassThroughStreamState(msComm, correlationID, expectedProviders, final)
	if !shouldForward {
		return nil, false, shouldStop, nil
	}
	addTrace(forwarded, span)
	return forwarded, shouldForward, shouldStop, nil
}

func addTrace(msComm *pb.MicroserviceCommunication, span *trace.Span) {
	if msComm == nil {
		return
	}
	if msComm.Traces == nil {
		msComm.Traces = map[string][]byte{}
	}
	msComm.Traces["binaryTrace"] = propagation.Binary(span.SpanContext())
}

func mergeData(msCommList []*pb.MicroserviceCommunication) *pb.MicroserviceCommunication {
	if len(msCommList) == 0 {
		return nil
	}

	mergedData := &structpb.Struct{
		Fields: make(map[string]*structpb.Value),
	}

	for _, msComm := range msCommList {
		if msComm == nil || msComm.Data == nil {
			continue
		}

		// Merge data into mergedData, checking for and handling identical fields.
		for key, value2 := range msComm.Data.GetFields() {
			if value1, exists := mergedData.Fields[key]; exists {
				// Handle merging identical fields. This example simply appends the lists.
				// Custom logic needed based on the actual data structure and requirements.
				if value1.GetListValue() != nil && value2.GetListValue() != nil {
					mergedList := append(append([]*structpb.Value(nil), value1.GetListValue().Values...), value2.GetListValue().Values...)
					mergedData.Fields[key] = structpb.NewListValue(&structpb.ListValue{Values: mergedList})
				} else {
					// For non-list values, decide how to merge. This example simply replaces the value.
					mergedData.Fields[key] = value2
				}
			} else {
				// If the field is not in data1, add it from data2.
				mergedData.Fields[key] = value2
			}
		}
	}

	// For now simply return the first msCommList with the merged data.
	msCommList[0].Data = mergedData
	return msCommList[0]
}

func updateAverageStreamState(msComm *pb.MicroserviceCommunication, correlationID string, expectedProviders int, final bool) (*pb.MicroserviceCommunication, bool, bool) {
	return updatePassThroughStreamState(msComm, correlationID, expectedProviders, final)
}

func updatePassThroughStreamState(msComm *pb.MicroserviceCommunication, correlationID string, expectedProviders int, final bool) (*pb.MicroserviceCommunication, bool, bool) {
	aggregateStateMu.Lock()
	defer aggregateStateMu.Unlock()

	state := getOrCreateAggregateState(correlationID)
	if markAggregateBatchSeen(state, msComm) {
		logger.Sugar().Warnw("Ignoring duplicate pass-through aggregate stream batch", "correlationId", correlationID, "batchId", streamBatchIdentity(msComm))
		return nil, false, false
	}

	state.nextSequence++
	state.rowsProcessed += structRowCount(msComm.GetData())
	if final {
		providerKey := streamProviderIdentity(state, msComm)
		if _, alreadyFinal := state.providerFinals[providerKey]; !alreadyFinal {
			state.providerFinals[providerKey] = struct{}{}
			state.rowsTotal += lib.MetadataInt(msComm.Metadata, lib.StreamRowsTotalMetadataKey)
		}
	}

	if msComm.Metadata == nil {
		msComm.Metadata = map[string]string{}
	}

	overallFinal := final && len(state.providerFinals) >= expectedProviders
	msComm.Metadata[lib.StreamSequenceMetadataKey] = strconv.Itoa(state.nextSequence)
	msComm.Metadata[lib.StreamRowsProcessedMetadataKey] = strconv.Itoa(state.rowsProcessed)
	msComm.Metadata[lib.StreamRowsTotalMetadataKey] = strconv.Itoa(state.rowsTotal)
	msComm.Metadata[lib.StreamPartialMetadataKey] = strconv.FormatBool(!overallFinal)
	msComm.Metadata[lib.StreamFinalMetadataKey] = strconv.FormatBool(overallFinal)

	if overallFinal {
		delete(aggregateStates, correlationID)
	}

	return msComm, true, overallFinal
}

func updateBufferedAggregateState(msComm *pb.MicroserviceCommunication, correlationID string, expectedProviders int, final bool) (*pb.MicroserviceCommunication, bool, bool) {
	aggregateStateMu.Lock()
	defer aggregateStateMu.Unlock()

	state := getOrCreateAggregateState(correlationID)
	if markAggregateBatchSeen(state, msComm) {
		logger.Sugar().Warnw("Ignoring duplicate buffered aggregate stream batch", "correlationId", correlationID, "batchId", streamBatchIdentity(msComm))
		return nil, false, false
	}

	state.buffered = append(state.buffered, msComm)
	if final {
		providerKey := streamProviderIdentity(state, msComm)
		state.providerFinals[providerKey] = struct{}{}
	}

	if len(state.providerFinals) < expectedProviders {
		return nil, false, false
	}

	merged := mergeData(state.buffered)
	if merged.Metadata == nil {
		merged.Metadata = map[string]string{}
	}
	rowCount := structRowCount(merged.GetData())
	merged.Metadata[lib.StreamSequenceMetadataKey] = strconv.Itoa(1)
	merged.Metadata[lib.StreamRowsProcessedMetadataKey] = strconv.Itoa(rowCount)
	merged.Metadata[lib.StreamRowsTotalMetadataKey] = strconv.Itoa(rowCount)
	merged.Metadata[lib.StreamPartialMetadataKey] = strconv.FormatBool(false)
	merged.Metadata[lib.StreamFinalMetadataKey] = strconv.FormatBool(true)
	delete(aggregateStates, correlationID)

	return merged, true, true
}

func getOrCreateAggregateState(correlationID string) *aggregateStreamState {
	state, ok := aggregateStates[correlationID]
	if ok {
		return state
	}

	state = &aggregateStreamState{
		providerFinals: map[string]struct{}{},
		seenBatches:    map[string]struct{}{},
	}
	aggregateStates[correlationID] = state
	return state
}

func markAggregateBatchSeen(state *aggregateStreamState, msComm *pb.MicroserviceCommunication) bool {
	if state == nil {
		return false
	}
	if state.seenBatches == nil {
		state.seenBatches = map[string]struct{}{}
	}
	if state.providerFinals == nil {
		state.providerFinals = map[string]struct{}{}
	}

	batchID := streamBatchIdentity(msComm)
	if batchID == "" {
		return false
	}
	if _, seen := state.seenBatches[batchID]; seen {
		return true
	}
	state.seenBatches[batchID] = struct{}{}
	return false
}

func streamBatchIdentity(msComm *pb.MicroserviceCommunication) string {
	if msComm == nil || msComm.Metadata == nil {
		return ""
	}
	if batchID := msComm.Metadata[lib.StreamBatchIDMetadataKey]; batchID != "" {
		return batchID
	}
	provider := msComm.Metadata[lib.StreamProviderMetadataKey]
	sequence := msComm.Metadata[lib.StreamSequenceMetadataKey]
	if provider == "" || sequence == "" {
		return ""
	}
	return provider + ":" + sequence
}

func streamProviderIdentity(state *aggregateStreamState, msComm *pb.MicroserviceCommunication) string {
	if msComm != nil && msComm.Metadata != nil {
		if provider := msComm.Metadata[lib.StreamProviderMetadataKey]; provider != "" {
			return provider
		}
		if batchID := streamBatchIdentity(msComm); batchID != "" {
			return batchID
		}
	}
	if state == nil {
		return "provider-1"
	}
	return "provider-" + strconv.Itoa(len(state.providerFinals)+1)
}

func aggregateCorrelationID(msComm *pb.MicroserviceCommunication) string {
	if msComm != nil && msComm.GetRequestMetadata() != nil && msComm.GetRequestMetadata().GetCorrelationId() != "" {
		return msComm.GetRequestMetadata().GetCorrelationId()
	}
	return "default"
}

func structRowCount(data *structpb.Struct) int {
	if data == nil {
		return 0
	}

	maxRows := 0
	for _, value := range data.GetFields() {
		listValue := value.GetListValue()
		if listValue == nil {
			continue
		}
		if rowCount := len(listValue.GetValues()); rowCount > maxRows {
			maxRows = rowCount
		}
	}

	return maxRows
}
