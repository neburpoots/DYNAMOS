package main

import (
	"bytes"
	"sync"
	"testing"

	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSplitAndReassembleMicroserviceCommunication(t *testing.T) {
	t.Setenv("RABBITMQ_STREAM_CHUNK_ROWS", "2")
	t.Setenv("RABBITMQ_STREAM_CHUNK_BYTES", "12")

	msComm := &pb.MicroserviceCommunication{
		Type:        "microserviceCommunication",
		RequestType: "sqlDataRequest",
		Data: &structpb.Struct{Fields: map[string]*structpb.Value{
			"Name": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
				structpb.NewStringValue("Ada"),
				structpb.NewStringValue("Grace"),
				structpb.NewStringValue("Edsger"),
			}}),
			"Score": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
				structpb.NewStringValue("1"),
				structpb.NewStringValue("2"),
				structpb.NewStringValue("3"),
			}}),
		}},
		Metadata: map[string]string{
			lib.TransportMetadataKey: lib.TransportRabbitMQStreams,
		},
		RequestMetadata: &pb.RequestMetadata{
			CorrelationId: "corr-1",
			Transport:     lib.TransportRabbitMQStreams,
		},
		Result: []byte("abcdefghijklmnopqrstuvwxyz"),
	}

	chunks, err := splitMicroserviceCommunication(msComm)
	if err != nil {
		t.Fatalf("splitMicroserviceCommunication() error = %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}

	acc := &microserviceChunkAccumulator{chunks: make(map[uint32]*pb.MicroserviceCommunicationChunk)}
	for _, chunk := range chunks {
		acc.chunks[chunk.GetChunkIndex()] = chunk
		if chunk.GetIsFinal() {
			acc.finalSeen = true
			acc.totalChunks = chunk.GetTotalChunks()
		}
	}

	reassembled, err := acc.reassemble()
	if err != nil {
		t.Fatalf("reassemble() error = %v", err)
	}

	if !bytes.Equal(reassembled.GetResult(), msComm.GetResult()) {
		t.Fatalf("reassembled result = %q, want %q", reassembled.GetResult(), msComm.GetResult())
	}
	if got := len(reassembled.GetData().GetFields()["Name"].GetListValue().GetValues()); got != 3 {
		t.Fatalf("reassembled row count = %d, want 3", got)
	}
	if got := reassembled.GetMetadata()[lib.TransportMetadataKey]; got != lib.TransportRabbitMQStreams {
		t.Fatalf("transport metadata = %q, want %q", got, lib.TransportRabbitMQStreams)
	}
}

func TestHandleRabbitMQStreamChunkDisambiguatesInterleavedMessages(t *testing.T) {
	t.Setenv("RABBITMQ_STREAM_CHUNK_ROWS", "1")
	t.Setenv("RABBITMQ_STREAM_CHUNK_BYTES", "4096")

	first := testStreamMicroserviceCommunication("shared-corr", []string{"Ada", "Grace"})
	second := testStreamMicroserviceCommunication("shared-corr", []string{"Linus", "Turing"})

	firstChunks, err := splitMicroserviceCommunication(first)
	if err != nil {
		t.Fatalf("split first message: %v", err)
	}
	secondChunks, err := splitMicroserviceCommunication(second)
	if err != nil {
		t.Fatalf("split second message: %v", err)
	}
	if len(firstChunks) != 2 || len(secondChunks) != 2 {
		t.Fatalf("expected two chunks per message, got first=%d second=%d", len(firstChunks), len(secondChunks))
	}

	buffers := map[string]*microserviceChunkAccumulator{}
	bufferMutex := &sync.Mutex{}

	if _, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessage(t, firstChunks[0], "first-msg"), buffers, bufferMutex); err != nil || complete {
		t.Fatalf("first chunk complete=%t err=%v, want incomplete nil", complete, err)
	}
	if _, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessage(t, secondChunks[0], "second-msg"), buffers, bufferMutex); err != nil || complete {
		t.Fatalf("second chunk complete=%t err=%v, want incomplete nil", complete, err)
	}

	reassembledFirst, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessage(t, firstChunks[1], "first-msg"), buffers, bufferMutex)
	if err != nil {
		t.Fatalf("final first chunk error: %v", err)
	}
	if !complete {
		t.Fatal("final first chunk did not complete the first message")
	}
	if got := streamNames(reassembledFirst); !equalStrings(got, []string{"Ada", "Grace"}) {
		t.Fatalf("first reassembled names = %v, want [Ada Grace]", got)
	}

	reassembledSecond, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessage(t, secondChunks[1], "second-msg"), buffers, bufferMutex)
	if err != nil {
		t.Fatalf("final second chunk error: %v", err)
	}
	if !complete {
		t.Fatal("final second chunk did not complete the second message")
	}
	if got := streamNames(reassembledSecond); !equalStrings(got, []string{"Linus", "Turing"}) {
		t.Fatalf("second reassembled names = %v, want [Linus Turing]", got)
	}
}

func TestHandleRabbitMQStreamChunkFallsBackToAMQPCorrelationID(t *testing.T) {
	t.Setenv("RABBITMQ_STREAM_CHUNK_ROWS", "1")
	t.Setenv("RABBITMQ_STREAM_CHUNK_BYTES", "4096")

	first := testStreamMicroserviceCommunication("shared-corr", []string{"Ada", "Grace"})
	second := testStreamMicroserviceCommunication("shared-corr", []string{"Linus", "Turing"})

	firstChunks, err := splitMicroserviceCommunication(first)
	if err != nil {
		t.Fatalf("split first message: %v", err)
	}
	secondChunks, err := splitMicroserviceCommunication(second)
	if err != nil {
		t.Fatalf("split second message: %v", err)
	}

	buffers := map[string]*microserviceChunkAccumulator{}
	bufferMutex := &sync.Mutex{}

	if _, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessageWithoutAppMessageID(t, firstChunks[0], "first-msg"), buffers, bufferMutex); err != nil || complete {
		t.Fatalf("first chunk complete=%t err=%v, want incomplete nil", complete, err)
	}
	if _, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessageWithoutAppMessageID(t, secondChunks[0], "second-msg"), buffers, bufferMutex); err != nil || complete {
		t.Fatalf("second chunk complete=%t err=%v, want incomplete nil", complete, err)
	}

	reassembledFirst, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessageWithoutAppMessageID(t, firstChunks[1], "first-msg"), buffers, bufferMutex)
	if err != nil {
		t.Fatalf("final first chunk error: %v", err)
	}
	if !complete {
		t.Fatal("final first chunk did not complete the first message")
	}
	if got := streamNames(reassembledFirst); !equalStrings(got, []string{"Ada", "Grace"}) {
		t.Fatalf("first reassembled names = %v, want [Ada Grace]", got)
	}

	reassembledSecond, complete, err := handleRabbitMQStreamChunk(testStreamAMQPMessageWithoutAppMessageID(t, secondChunks[1], "second-msg"), buffers, bufferMutex)
	if err != nil {
		t.Fatalf("final second chunk error: %v", err)
	}
	if !complete {
		t.Fatal("final second chunk did not complete the second message")
	}
	if got := streamNames(reassembledSecond); !equalStrings(got, []string{"Linus", "Turing"}) {
		t.Fatalf("second reassembled names = %v, want [Linus Turing]", got)
	}
}

func TestHandleRabbitMQStreamChunkDefersOffsetCommitForInterleavedPartial(t *testing.T) {
	t.Setenv("RABBITMQ_STREAM_CHUNK_ROWS", "1")
	t.Setenv("RABBITMQ_STREAM_CHUNK_BYTES", "4096")

	first := testStreamMicroserviceCommunication("shared-corr", []string{"Ada", "Grace"})
	second := testStreamMicroserviceCommunication("shared-corr", []string{"Linus", "Turing"})

	firstChunks, err := splitMicroserviceCommunication(first)
	if err != nil {
		t.Fatalf("split first message: %v", err)
	}
	secondChunks, err := splitMicroserviceCommunication(second)
	if err != nil {
		t.Fatalf("split second message: %v", err)
	}

	buffers := map[string]*microserviceChunkAccumulator{}
	bufferMutex := &sync.Mutex{}

	if _, complete, safeToCommit, err := handleRabbitMQStreamChunkAtOffset(testStreamAMQPMessage(t, firstChunks[0], "first-msg"), 10, buffers, bufferMutex); err != nil || complete || safeToCommit {
		t.Fatalf("first chunk complete=%t safeToCommit=%t err=%v, want incomplete unsafe nil", complete, safeToCommit, err)
	}
	if _, complete, safeToCommit, err := handleRabbitMQStreamChunkAtOffset(testStreamAMQPMessage(t, secondChunks[0], "second-msg"), 11, buffers, bufferMutex); err != nil || complete || safeToCommit {
		t.Fatalf("second chunk complete=%t safeToCommit=%t err=%v, want incomplete unsafe nil", complete, safeToCommit, err)
	}
	if _, complete, safeToCommit, err := handleRabbitMQStreamChunkAtOffset(testStreamAMQPMessage(t, firstChunks[1], "first-msg"), 12, buffers, bufferMutex); err != nil || !complete || safeToCommit {
		t.Fatalf("first final complete=%t safeToCommit=%t err=%v, want complete unsafe nil", complete, safeToCommit, err)
	}
	if _, complete, safeToCommit, err := handleRabbitMQStreamChunkAtOffset(testStreamAMQPMessage(t, secondChunks[1], "second-msg"), 13, buffers, bufferMutex); err != nil || !complete || !safeToCommit {
		t.Fatalf("second final complete=%t safeToCommit=%t err=%v, want complete safe nil", complete, safeToCommit, err)
	}
}

func testStreamMicroserviceCommunication(correlationID string, names []string) *pb.MicroserviceCommunication {
	values := make([]*structpb.Value, 0, len(names))
	for _, name := range names {
		values = append(values, structpb.NewStringValue(name))
	}
	return &pb.MicroserviceCommunication{
		Type:        "microserviceCommunication",
		RequestType: "sqlDataRequest",
		Data: &structpb.Struct{Fields: map[string]*structpb.Value{
			"Name": structpb.NewListValue(&structpb.ListValue{Values: values}),
		}},
		Metadata: map[string]string{
			lib.TransportMetadataKey: lib.TransportRabbitMQStreams,
		},
		RequestMetadata: &pb.RequestMetadata{
			CorrelationId: correlationID,
			Transport:     lib.TransportRabbitMQStreams,
		},
	}
}

func testStreamAMQPMessage(t *testing.T, chunk *pb.MicroserviceCommunicationChunk, messageID string) *streamamqp.Message {
	t.Helper()
	body, err := proto.Marshal(chunk)
	if err != nil {
		t.Fatalf("marshal chunk: %v", err)
	}
	message := &streamamqp.Message{
		Data: [][]byte{body},
	}
	message.ApplicationProperties = map[string]any{
		streamCorrelationIDAppProperty: chunk.GetCorrelationId(),
		streamMessageIDAppProperty:     messageID,
		streamChunkIndexAppProperty:    int64(chunk.GetChunkIndex()),
		streamTotalChunksAppProperty:   int64(chunk.GetTotalChunks()),
		streamIsFinalAppProperty:       chunk.GetIsFinal(),
	}
	message.Properties = &streamamqp.MessageProperties{CorrelationID: messageID}
	return message
}

func testStreamAMQPMessageWithoutAppMessageID(t *testing.T, chunk *pb.MicroserviceCommunicationChunk, messageID string) *streamamqp.Message {
	t.Helper()
	message := testStreamAMQPMessage(t, chunk, messageID)
	delete(message.ApplicationProperties, streamMessageIDAppProperty)
	return message
}

func streamNames(msComm *pb.MicroserviceCommunication) []string {
	if msComm == nil || msComm.GetData() == nil {
		return nil
	}
	nameField := msComm.GetData().GetFields()["Name"]
	if nameField == nil || nameField.GetListValue() == nil {
		return nil
	}
	values := nameField.GetListValue().GetValues()
	names := make([]string, 0, len(values))
	for _, value := range values {
		names = append(names, value.GetStringValue())
	}
	return names
}

func equalStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
