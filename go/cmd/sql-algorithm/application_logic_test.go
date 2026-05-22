package main

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGetAverageIgnoresDuplicateStreamBatch(t *testing.T) {
	averageAccumulatorMu.Lock()
	averageAccumulators = map[string]*averageAccumulator{}
	averageAccumulatorMu.Unlock()

	first := testAlgorithmAverageMessage("corr-1", "UVA", 1, false, []string{"M", "V"}, []string{"10", "20"})
	_ = getAverage(first, false)

	duplicate := testAlgorithmAverageMessage("corr-1", "UVA", 1, false, []string{"M", "V"}, []string{"10", "20"})
	_ = getAverage(duplicate, false)

	final := testAlgorithmAverageMessage("corr-1", "VU", 1, true, []string{"M", "V"}, []string{"30", "40"})
	result := getAverage(final, true)

	var decoded map[string]string
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatalf("unmarshal average result: %v", err)
	}
	if got := decoded["avg_salary_scale_men"]; got != "20.000" {
		t.Fatalf("male average = %q, want 20.000", got)
	}
	if got := decoded["avg_salary_scale_women"]; got != "30.000" {
		t.Fatalf("female average = %q, want 30.000", got)
	}
}

func TestConvertAllDataUsesStreamColumnOrder(t *testing.T) {
	data := &structpb.Struct{Fields: map[string]*structpb.Value{
		"Geslacht": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStringValue("V"),
		}}),
		"Unieknr": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
			structpb.NewStringValue("10000001"),
		}}),
	}}
	_, result := convertAllData(context.Background(), data, map[string]string{
		lib.StreamColumnsMetadataKey: `["Unieknr","Geslacht"]`,
	})

	var decoded [][]string
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatalf("unmarshal converted rows: %v", err)
	}
	if got := decoded[0]; len(got) != 2 || got[0] != "Unieknr" || got[1] != "Geslacht" {
		t.Fatalf("header = %#v, want [Unieknr Geslacht]", got)
	}
	if got := decoded[1]; len(got) != 2 || got[0] != "10000001" || got[1] != "V" {
		t.Fatalf("row = %#v, want [10000001 V]", got)
	}
}

func TestHandleSqlDataRequestBulkStopsOnlyOnFinal(t *testing.T) {
	partial := testAlgorithmBulkMessage("corr-bulk", false)
	shouldStop, err := handleSqlDataRequest(context.Background(), partial)
	if err != nil {
		t.Fatalf("partial bulk returned error: %v", err)
	}
	if shouldStop {
		t.Fatalf("partial bulk shouldStop = true, want false")
	}

	final := testAlgorithmBulkMessage("corr-bulk", true)
	shouldStop, err = handleSqlDataRequest(context.Background(), final)
	if err != nil {
		t.Fatalf("final bulk returned error: %v", err)
	}
	if !shouldStop {
		t.Fatalf("final bulk shouldStop = false, want true")
	}
}

func testAlgorithmAverageMessage(correlationID string, provider string, sequence int, final bool, genders []string, salaries []string) *pb.MicroserviceCommunication {
	genderValues := make([]*structpb.Value, 0, len(genders))
	for _, gender := range genders {
		genderValues = append(genderValues, structpb.NewStringValue(gender))
	}
	salaryValues := make([]*structpb.Value, 0, len(salaries))
	for _, salary := range salaries {
		salaryValues = append(salaryValues, structpb.NewStringValue(salary))
	}

	return &pb.MicroserviceCommunication{
		Data: &structpb.Struct{Fields: map[string]*structpb.Value{
			"Geslacht": structpb.NewListValue(&structpb.ListValue{Values: genderValues}),
			"Salschal": structpb.NewListValue(&structpb.ListValue{Values: salaryValues}),
		}},
		Metadata: map[string]string{
			lib.StreamProviderMetadataKey: provider,
			lib.StreamBatchIDMetadataKey:  provider + ":" + strconv.Itoa(sequence),
			lib.StreamSequenceMetadataKey: strconv.Itoa(sequence),
			lib.StreamPartialMetadataKey:  strconv.FormatBool(!final),
			lib.StreamFinalMetadataKey:    strconv.FormatBool(final),
		},
		RequestMetadata: &pb.RequestMetadata{CorrelationId: correlationID},
	}
}

func testAlgorithmBulkMessage(correlationID string, final bool) *pb.MicroserviceCommunication {
	requestBody, err := anypb.New(&pb.SqlDataRequest{
		Algorithm: "rows",
		Options:   map[string]bool{},
	})
	if err != nil {
		panic(err)
	}

	return &pb.MicroserviceCommunication{
		Data: &structpb.Struct{Fields: map[string]*structpb.Value{
			"Unieknr": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
				structpb.NewStringValue("10000001"),
			}}),
			"Geslacht": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
				structpb.NewStringValue("V"),
			}}),
		}},
		Metadata: map[string]string{
			lib.StreamColumnsMetadataKey:       `["Unieknr","Geslacht"]`,
			lib.StreamProviderMetadataKey:      "UVA",
			lib.StreamBatchIDMetadataKey:       "UVA:1",
			lib.StreamSequenceMetadataKey:      "1",
			lib.StreamPartialMetadataKey:       strconv.FormatBool(!final),
			lib.StreamFinalMetadataKey:         strconv.FormatBool(final),
			lib.StreamRowsProcessedMetadataKey: "1",
			lib.StreamRowsTotalMetadataKey:     "1",
		},
		OriginalRequest: requestBody,
		RequestMetadata: &pb.RequestMetadata{CorrelationId: correlationID},
		Traces:          map[string][]byte{},
	}
}
