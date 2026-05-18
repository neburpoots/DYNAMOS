package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"

	"github.com/gogo/protobuf/jsonpb"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
	"google.golang.org/protobuf/types/known/structpb"
)

type averageAccumulator struct {
	totalMaleSalary   float64
	totalFemaleSalary float64
	maleCount         int
	femaleCount       int
	seenBatches       map[string]struct{}
}

var (
	averageAccumulatorMu sync.Mutex
	averageAccumulators  = map[string]*averageAccumulator{}
)

// // func ReveiceData() {
// // 	// Assuming `row` is your data fetched from the database.
// // 	fields := make(map[string]*structpb.Value)
// // 	fields["name"] = structpb.NewStringValue("Jorrit")
// // 	fields["date_of_birth"] = structpb.NewStringValue("september")
// // 	fields["job_title"] = structpb.NewStringValue("IT")
// // 	fields["other"] = structpb.NewBoolValue(true)

// // 	s := &structpb.Struct{Fields: fields}

// // 	iets := s.Fields["other"].GetListValue().ProtoReflect().Type()
// // 	fmt.Println(iets)
// // 	fmt.Println("xxx")
// // 	fmt.Println(s.GetFields())
// // }

// This is the function being called by the last microservice
func handleSqlDataRequest(ctx context.Context, msComm *pb.MicroserviceCommunication) (bool, error) {
	ctx, span := trace.StartSpan(ctx, "handleSqlDataRequest")
	defer span.End()

	logger.Info("Start handleSqlDataRequest")
	// Unpack the metadata
	metadata := msComm.Metadata
	// fields := make(map[string]*structpb.Value)
	// dataField := msComm.GetData()
	// Get the "Functcat" field from the struct
	// functcatValue := dataField.Fields["HOOPgeb"]

	// // Check if it's a ListValue
	// if functcatValue != nil {
	// 	if listValue, ok := functcatValue.Kind.(*structpb.Value_ListValue); ok {
	// 		// Iterate over the Values in the ListValue
	// 		for _, item := range listValue.ListValue.GetValues() {
	// 			// item is a *structpb.Value, so we need to get the actual value using one of its getter methods
	// 			switch v := item.Kind.(type) {
	// 			case *structpb.Value_StringValue:
	// 				fmt.Printf("String value: %s\n", v.StringValue)
	// 			case *structpb.Value_NumberValue:
	// 				fmt.Printf("Number value: %f\n", v.NumberValue)
	// 			case *structpb.Value_BoolValue:
	// 				fmt.Printf("Bool value: %v\n", v.BoolValue)
	// 			// etc. for other possible types
	// 			default:
	// 				fmt.Printf("Other value: %v\n", v)
	// 			}
	// 		}
	// 	}
	// }

	// Print each metadata field
	logger.Sugar().Debugf("Length metadata: %s", strconv.Itoa(len(metadata)))
	// for key, value := range metadata {
	// 	fmt.Printf("Key: %s, Value: %+v\n", key, value)
	// }

	sqlDataRequest := &pb.SqlDataRequest{}
	if err := msComm.OriginalRequest.UnmarshalTo(sqlDataRequest); err != nil {
		logger.Sugar().Errorf("Failed to unmarshal sqlDataRequest message: %v", err)
	}

	msComm.Traces["binaryTrace"] = propagation.Binary(span.SpanContext())
	final := lib.MetadataBool(msComm.Metadata, lib.StreamFinalMetadataKey, true)

	if sqlDataRequest.Options["graph"] {
		// jsonString, _ := json.Marshal(msComm.Data)
		// msComm.Result = jsonString

		m := &jsonpb.Marshaler{}
		jsonString, _ := m.MarshalToString(msComm.Data)
		msComm.Result = []byte(jsonString)

		return true, nil
	}

	if sqlDataRequest.Algorithm == "average" {
		msComm.Result = getAverage(msComm, final)
		msComm.Data = nil
		if msComm.Metadata == nil {
			msComm.Metadata = map[string]string{}
		}
		msComm.Metadata[lib.StreamPartialMetadataKey] = strconv.FormatBool(!final)
		msComm.Metadata[lib.StreamFinalMetadataKey] = strconv.FormatBool(final)
		return final, nil
	}

	// // Just pass on the data for now...
	// if config.LastService {
	// 	msComm.Result = getAverage(msComm.Data)
	// }

	// Process all data to make this service more realistic.
	ctx, allResults := convertAllData(ctx, msComm.Data)
	msComm.Result = allResults
	msComm.Data = nil

	return true, nil
}

func convertAllData(ctx context.Context, data *structpb.Struct) (context.Context, []byte) {
	ctx, span := trace.StartSpan(ctx, "convertAllData")
	defer span.End()
	keys := make([]string, 0)
	allValues := make([][]string, 0)
	maxLength := 0

	for key, value := range data.GetFields() {
		stringValues := value.GetListValue().GetValues()
		if len(stringValues) > 0 {
			keys = append(keys, key)
			rowValues := make([]string, len(stringValues))
			for i, v := range stringValues {
				rowValues[i] = v.GetStringValue()
			}
			allValues = append(allValues, rowValues)
			if len(rowValues) > maxLength {
				maxLength = len(rowValues)
			}
		}
	}

	result := make([][]string, maxLength+1)
	result[0] = keys
	for i := 1; i < maxLength+1; i++ {
		row := make([]string, len(keys))
		for j := 0; j < len(keys); j++ {
			if i <= len(allValues[j]) {
				row[j] = allValues[j][i-1]
			} else {
				row[j] = ""
			}
		}
		result[i] = row
	}

	jsonData, err := json.Marshal(result)
	if err != nil {
		fmt.Printf("Error while marshalling to JSON: %v\n", err)
		return ctx, nil
	}

	return ctx, jsonData
}

func getFirstRow(data *structpb.Struct) []byte {
	keys := make([]string, 0)
	values := make([]string, 0)
	for key, value := range data.GetFields() {
		stringValues := value.GetListValue().GetValues()
		if len(stringValues) > 0 {
			keys = append(keys, key)
			values = append(values, stringValues[0].GetStringValue())
		}
	}

	// Convert to JSON format
	result := []interface{}{keys, values}
	jsonData, err := json.Marshal(result)
	if err != nil {
		fmt.Printf("Error while marshalling to JSON: %v\n", err)
		return nil
	}

	return jsonData
}

func getAverage(msComm *pb.MicroserviceCommunication, final bool) []byte {
	correlationID := ""
	if msComm.GetRequestMetadata() != nil {
		correlationID = msComm.GetRequestMetadata().GetCorrelationId()
	}

	averageAccumulatorMu.Lock()
	defer averageAccumulatorMu.Unlock()

	accumulator := &averageAccumulator{}
	if correlationID != "" {
		storedAccumulator, ok := averageAccumulators[correlationID]
		if !ok {
			storedAccumulator = &averageAccumulator{}
			averageAccumulators[correlationID] = storedAccumulator
		}
		accumulator = storedAccumulator
	}

	if markAverageBatchSeen(accumulator, msComm) {
		logger.Sugar().Warnw("Ignoring duplicate algorithm average stream batch", "correlationId", correlationID, "batchId", averageBatchIdentity(msComm))
	} else {
		updateAverageAccumulator(accumulator, msComm.Data)
	}
	jsonResult := marshalAverageAccumulator(accumulator)

	if final && correlationID != "" {
		delete(averageAccumulators, correlationID)
	}

	return jsonResult
}

func markAverageBatchSeen(accumulator *averageAccumulator, msComm *pb.MicroserviceCommunication) bool {
	if accumulator == nil {
		return false
	}
	if accumulator.seenBatches == nil {
		accumulator.seenBatches = map[string]struct{}{}
	}
	batchID := averageBatchIdentity(msComm)
	if batchID == "" {
		return false
	}
	if _, seen := accumulator.seenBatches[batchID]; seen {
		return true
	}
	accumulator.seenBatches[batchID] = struct{}{}
	return false
}

func averageBatchIdentity(msComm *pb.MicroserviceCommunication) string {
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

func updateAverageAccumulator(accumulator *averageAccumulator, data *structpb.Struct) {
	if accumulator == nil || data == nil {
		return
	}

	gendersField, ok1 := data.GetFields()["Geslacht"]
	salariesField, ok2 := data.GetFields()["Salschal"]
	if !ok1 || !ok2 {
		logger.Error("Genders or Salaries field not found")
		return
	}

	genders := gendersField.GetListValue().GetValues()
	salaries := salariesField.GetListValue().GetValues()
	for index, gender := range genders {
		if index >= len(salaries) {
			break
		}

		genderStr := gender.GetStringValue()
		salaryStr := salaries[index].GetStringValue()
		if salaryStr == "" {
			continue
		}

		salary, err := strconv.ParseFloat(salaryStr, 64)
		if err != nil {
			fmt.Printf("Error parsing salary value: %v\n", err)
			continue
		}

		switch genderStr {
		case "M":
			accumulator.totalMaleSalary += salary
			accumulator.maleCount++
		case "V":
			accumulator.totalFemaleSalary += salary
			accumulator.femaleCount++
		}
	}
}

func marshalAverageAccumulator(accumulator *averageAccumulator) []byte {
	result := make(map[string]string)
	if accumulator.maleCount != 0 {
		result["avg_salary_scale_men"] = fmt.Sprintf("%.3f", accumulator.totalMaleSalary/float64(accumulator.maleCount))
	}
	if accumulator.femaleCount != 0 {
		result["avg_salary_scale_women"] = fmt.Sprintf("%.3f", accumulator.totalFemaleSalary/float64(accumulator.femaleCount))
	}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		logger.Sugar().Error(err)
		return nil
	}

	return jsonResult
}
