package main

import (
	"strconv"
	"testing"

	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestUpdateAverageStreamStateIgnoresDuplicateBatch(t *testing.T) {
	aggregateStateMu.Lock()
	aggregateStates = map[string]*aggregateStreamState{}
	aggregateStateMu.Unlock()

	first := testAggregateStreamMessage("corr-1", "UVA", 1, false, 2)
	forwarded, shouldForward, shouldStop := updateAverageStreamState(first, "corr-1", 2, false)
	if !shouldForward || shouldStop {
		t.Fatalf("first batch shouldForward=%t shouldStop=%t, want true false", shouldForward, shouldStop)
	}
	if got := forwarded.Metadata[lib.StreamRowsProcessedMetadataKey]; got != "2" {
		t.Fatalf("rows processed after first batch = %q, want 2", got)
	}

	duplicate := testAggregateStreamMessage("corr-1", "UVA", 1, false, 2)
	_, shouldForward, shouldStop = updateAverageStreamState(duplicate, "corr-1", 2, false)
	if shouldForward || shouldStop {
		t.Fatalf("duplicate batch shouldForward=%t shouldStop=%t, want false false", shouldForward, shouldStop)
	}

	uvaFinal := testAggregateStreamMessage("corr-1", "UVA", 2, true, 0)
	uvaFinal.Metadata[lib.StreamRowsTotalMetadataKey] = "2"
	forwarded, shouldForward, shouldStop = updateAverageStreamState(uvaFinal, "corr-1", 2, true)
	if !shouldForward || shouldStop {
		t.Fatalf("first provider final shouldForward=%t shouldStop=%t, want true false", shouldForward, shouldStop)
	}
	if got := forwarded.Metadata[lib.StreamRowsTotalMetadataKey]; got != "2" {
		t.Fatalf("rows total after first provider final = %q, want 2", got)
	}

	vuFinal := testAggregateStreamMessage("corr-1", "VU", 1, true, 2)
	vuFinal.Metadata[lib.StreamRowsTotalMetadataKey] = "2"
	forwarded, shouldForward, shouldStop = updateAverageStreamState(vuFinal, "corr-1", 2, true)
	if !shouldForward || !shouldStop {
		t.Fatalf("second provider final shouldForward=%t shouldStop=%t, want true true", shouldForward, shouldStop)
	}
	if got := forwarded.Metadata[lib.StreamRowsProcessedMetadataKey]; got != "4" {
		t.Fatalf("rows processed after duplicate and final = %q, want 4", got)
	}
	if got := forwarded.Metadata[lib.StreamRowsTotalMetadataKey]; got != "4" {
		t.Fatalf("rows total after duplicate and final = %q, want 4", got)
	}
}

func TestMergeDataDoesNotDuplicateFirstMessage(t *testing.T) {
	first := testAggregateStreamMessage("corr-1", "UVA", 1, true, 2)
	second := testAggregateStreamMessage("corr-1", "VU", 1, true, 3)

	merged := mergeData([]*pb.MicroserviceCommunication{first, second})
	if got := structRowCount(merged.GetData()); got != 5 {
		t.Fatalf("merged row count = %d, want 5", got)
	}
}

func testAggregateStreamMessage(correlationID string, provider string, sequence int, final bool, rows int) *pb.MicroserviceCommunication {
	genders := make([]*structpb.Value, 0, rows)
	salaries := make([]*structpb.Value, 0, rows)
	for index := 0; index < rows; index++ {
		gender := "M"
		if index%2 == 1 {
			gender = "V"
		}
		genders = append(genders, structpb.NewStringValue(gender))
		salaries = append(salaries, structpb.NewStringValue("12"))
	}

	return &pb.MicroserviceCommunication{
		Data: &structpb.Struct{Fields: map[string]*structpb.Value{
			"Geslacht": structpb.NewListValue(&structpb.ListValue{Values: genders}),
			"Salschal": structpb.NewListValue(&structpb.ListValue{Values: salaries}),
		}},
		Metadata: map[string]string{
			lib.StreamProviderMetadataKey:      provider,
			lib.StreamBatchIDMetadataKey:       provider + ":" + strconv.Itoa(sequence),
			lib.StreamSequenceMetadataKey:      strconv.Itoa(sequence),
			lib.StreamPartialMetadataKey:       strconvBool(!final),
			lib.StreamFinalMetadataKey:         strconvBool(final),
			lib.StreamRowsProcessedMetadataKey: strconv.Itoa(rows),
			lib.StreamRowsTotalMetadataKey:     strconv.Itoa(rows),
		},
		RequestMetadata: &pb.RequestMetadata{CorrelationId: correlationID},
	}
}

func strconvBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
