package main

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
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
