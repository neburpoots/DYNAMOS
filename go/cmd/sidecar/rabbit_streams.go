package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
	"github.com/google/uuid"
	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	messagepkg "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultStreamChunkRows          = 100
	defaultStreamChunkBytes         = 64 * 1024
	defaultStreamBatchSize          = 100
	defaultStreamBatchDelay         = 50
	defaultStreamMaxProducers       = 16
	defaultStreamMaxConsumers       = 4
	streamAutoCommitCount           = 100
	streamAutoCommitAfter           = 5 * time.Second
	streamInitialCredits            = 64
	defaultStreamPublishTimeoutSecs = 30
	streamMessageIDAppProperty      = "stream_message_id"
	streamCorrelationIDAppProperty  = "correlation_id"
	streamChunkIndexAppProperty     = "chunk_index"
	streamTotalChunksAppProperty    = "total_chunks"
	streamIsFinalAppProperty        = "is_final"
)

type microserviceChunkAccumulator struct {
	chunks        map[uint32]*pb.MicroserviceCommunicationChunk
	offsets       map[uint32]int64
	finalSeen     bool
	totalChunks   uint32
	correlationID string
	messageID     string
}

func shouldSendThroughRabbitMQStream(data *pb.MicroserviceCommunication, target string) bool {
	if data == nil || data.RequestMetadata == nil || !isAgentIngressQueue(target) {
		return false
	}
	return lib.IsRabbitMQStreamingTransport(resolveMicroserviceTransport(data))
}

func isAgentIngressQueue(queueName string) bool {
	return strings.HasSuffix(queueName, "-in")
}

func resolveMicroserviceTransport(data *pb.MicroserviceCommunication) string {
	if data == nil {
		return lib.TransportUnary
	}
	if data.RequestMetadata != nil && data.RequestMetadata.Transport != "" {
		return lib.NormalizeTransport(data.RequestMetadata.Transport)
	}
	if data.Metadata != nil && data.Metadata[lib.TransportMetadataKey] != "" {
		return lib.NormalizeTransport(data.Metadata[lib.TransportMetadataKey])
	}
	return lib.TransportUnary
}

func rabbitStreamName(queueName string) string {
	return queueName + "-stream"
}

func openRabbitMQStreamEnvironment() (*stream.Environment, error) {
	user := os.Getenv("AMQ_USER")
	pw := os.Getenv("AMQ_PASSWORD")

	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetHost(rabbitDNS).
		SetPort(rabbitStreamPort).
		SetUser(user).
		SetPassword(pw).
		SetVHost("/").
		SetMaxProducersPerClient(envInt("RABBITMQ_STREAM_MAX_PRODUCERS_PER_CLIENT", defaultStreamMaxProducers)).
		SetMaxConsumersPerClient(envInt("RABBITMQ_STREAM_MAX_CONSUMERS_PER_CLIENT", defaultStreamMaxConsumers)))
	if err != nil {
		return nil, fmt.Errorf("open rabbitmq stream environment: %w", err)
	}
	return env, nil
}

func declareRabbitMQStream(env *stream.Environment, streamName string) error {
	if err := env.DeclareStream(streamName, stream.NewStreamOptions().SetMaxLengthBytes(stream.ByteCapacity{}.GB(2))); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already") {
			return nil
		}
		return fmt.Errorf("declare stream %q: %w", streamName, err)
	}
	return nil
}

func (s *serverInstance) getOrCreateRabbitMQStreamProducer(streamName string) (*stream.Producer, error) {
	s.streamProducerMu.Lock()
	defer s.streamProducerMu.Unlock()

	if s.streamProducers == nil {
		s.streamProducers = make(map[string]*stream.Producer)
	}
	if producer, ok := s.streamProducers[streamName]; ok {
		return producer, nil
	}

	if s.streamEnv == nil {
		env, err := openRabbitMQStreamEnvironment()
		if err != nil {
			return nil, err
		}
		s.streamEnv = env
	}

	if err := declareRabbitMQStream(s.streamEnv, streamName); err != nil {
		return nil, err
	}

	producer, err := s.streamEnv.NewProducer(
		streamName,
		stream.NewProducerOptions().
			SetBatchSize(defaultStreamBatchSize).
			SetBatchPublishingDelay(defaultStreamBatchDelay),
	)
	if err != nil {
		return nil, fmt.Errorf("create stream producer %q: %w", streamName, err)
	}

	confirmCh := producer.NotifyPublishConfirmation()
	closeCh := producer.NotifyClose()
	go s.watchRabbitMQStreamPublishConfirmations(streamName, producer, confirmCh, closeCh)

	s.streamProducers[streamName] = producer
	return producer, nil
}

func (s *serverInstance) watchRabbitMQStreamPublishConfirmations(streamName string, producer *stream.Producer, confirmCh stream.ChannelPublishConfirm, closeCh stream.ChannelClose) {
	for {
		select {
		case statuses, ok := <-confirmCh:
			if !ok {
				return
			}
			for _, status := range statuses {
				if status == nil {
					continue
				}
				appProps := status.GetMessage().GetApplicationProperties()
				correlationID := appString(appProps, streamCorrelationIDAppProperty)
				messageID := streamMessageIDFromStreamMessage(status.GetMessage())
				chunkIndex := appProps[streamChunkIndexAppProperty]
				totalChunks := appProps[streamTotalChunksAppProperty]
				publishID := status.GetPublishingId()

				if status.IsConfirmed() && status.GetError() == nil {
					logger.Sugar().Debugw(
						"RabbitMQ stream publish confirmed",
						"stream", streamName,
						"correlation_id", correlationID,
						"message_id", messageID,
						"chunk_index", chunkIndex,
						"total_chunks", totalChunks,
						"publishing_id", publishID,
					)
					s.completeStreamPublish(publishID, nil)
					continue
				}

				err := status.GetError()
				if err == nil {
					err = fmt.Errorf("publish not confirmed (code=%d)", status.GetErrorCode())
				}
				logger.Sugar().Warnw(
					"RabbitMQ stream publish not confirmed",
					"stream", streamName,
					"correlation_id", correlationID,
					"message_id", messageID,
					"chunk_index", chunkIndex,
					"total_chunks", totalChunks,
					"publishing_id", publishID,
					"error", err,
					"error_code", status.GetErrorCode(),
				)
				s.completeStreamPublish(publishID, err)
			}
		case event, ok := <-closeCh:
			if !ok {
				s.removeRabbitMQStreamProducer(streamName, producer)
				return
			}
			s.removeRabbitMQStreamProducer(streamName, producer)
			logger.Sugar().Infow(
				"RabbitMQ stream producer closed",
				"stream", streamName,
				"name", event.Name,
				"reason", event.Reason,
				"error", event.Err,
			)
			return
		}
	}
}

// completeStreamPublish records the outcome of a single publishing-id. When all chunks
// of a registered msComm-batch have been confirmed (or any one of them has errored),
// the registered done-channel is signalled exactly once so the caller of
// SendDataThroughRabbitMQStream can unblock.
func (s *serverInstance) completeStreamPublish(publishingID int64, err error) {
	s.streamPendingMu.Lock()
	pending, ok := s.streamPending[publishingID]
	if ok {
		delete(s.streamPending, publishingID)
	}
	s.streamPendingMu.Unlock()

	if !ok || pending == nil {
		return
	}

	pending.mu.Lock()
	defer pending.mu.Unlock()
	if pending.completed {
		return
	}

	if err != nil {
		pending.completed = true
		s.cancelStreamPublish(pending.ids)
		select {
		case pending.done <- err:
		default:
		}
		return
	}

	pending.remaining--
	if pending.remaining <= 0 {
		pending.completed = true
		select {
		case pending.done <- nil:
		default:
		}
	}
}

func (s *serverInstance) registerStreamPublish(publishingIDs []int64) chan error {
	done := make(chan error, 1)
	pending := &pendingStreamPublish{ids: append([]int64(nil), publishingIDs...), remaining: len(publishingIDs), done: done}

	s.streamPendingMu.Lock()
	if s.streamPending == nil {
		s.streamPending = make(map[int64]*pendingStreamPublish)
	}
	for _, id := range publishingIDs {
		s.streamPending[id] = pending
	}
	s.streamPendingMu.Unlock()

	return done
}

func (s *serverInstance) cancelStreamPublish(publishingIDs []int64) {
	s.streamPendingMu.Lock()
	for _, id := range publishingIDs {
		delete(s.streamPending, id)
	}
	s.streamPendingMu.Unlock()
}

func (s *serverInstance) removeRabbitMQStreamProducer(streamName string, producer *stream.Producer) {
	s.streamProducerMu.Lock()
	defer s.streamProducerMu.Unlock()
	if s.streamProducers == nil {
		return
	}
	current, ok := s.streamProducers[streamName]
	if !ok || current != producer {
		return
	}
	delete(s.streamProducers, streamName)
}

func (s *serverInstance) closeRabbitMQStreamResources() {
	s.streamProducerMu.Lock()
	producers := s.streamProducers
	env := s.streamEnv
	s.streamProducers = nil
	s.streamEnv = nil
	s.streamProducerMu.Unlock()

	for streamName, producer := range producers {
		if producer == nil {
			continue
		}
		if err := producer.Close(); err != nil {
			logger.Sugar().Warnw("Failed to close RabbitMQ stream producer", "stream", streamName, "error", err)
		}
	}

	if env != nil {
		if err := env.Close(); err != nil {
			logger.Sugar().Warnw("Failed to close RabbitMQ stream environment", "error", err)
		}
	}
}

func SendDataThroughRabbitMQStream(ctx context.Context, data *pb.MicroserviceCommunication, target string, s *serverInstance) (*emptypb.Empty, error) {
	logger.Sugar().Infow("Sending microservice communication through RabbitMQ Streams", "target", target)
	running_messages += 1
	defer func() {
		running_messages -= 1
	}()

	streamName := rabbitStreamName(target)
	producer, err := s.getOrCreateRabbitMQStreamProducer(streamName)
	if err != nil {
		logger.Sugar().Errorf("Failed to get RabbitMQ stream producer: %v", err)
		return &emptypb.Empty{}, err
	}

	chunks, err := splitMicroserviceCommunication(data)
	if err != nil {
		logger.Sugar().Errorf("Failed to split microservice communication: %v", err)
		return &emptypb.Empty{}, err
	}

	// message_id uniquely identifies this msComm-publish among all chunks the sidecar
	// emits for the same correlation_id. Without it, the consumer accumulator keyed on
	// correlation_id would collide when multiple providers (or multiple sequential
	// partial msComms from one provider) share the same request correlation_id. The
	// id lives only in AMQP application properties so it does not require a proto change.
	messageID := uuid.NewString()
	correlationID := data.GetRequestMetadata().GetCorrelationId()
	publishingIDs := make([]int64, 0, len(chunks))
	messages := make([]*streamamqp.AMQP10, 0, len(chunks))
	for _, chunk := range chunks {
		body, err := proto.Marshal(chunk)
		if err != nil {
			return &emptypb.Empty{}, fmt.Errorf("marshal stream chunk: %w", err)
		}

		message := streamamqp.NewMessage(body)
		message.Properties = &streamamqp.MessageProperties{
			MessageID:     fmt.Sprintf("%s-%s-%d", correlationID, messageID, chunk.GetChunkIndex()),
			CorrelationID: messageID,
		}
		message.ApplicationProperties = map[string]any{
			streamCorrelationIDAppProperty: correlationID,
			streamMessageIDAppProperty:     messageID,
			streamChunkIndexAppProperty:    int64(chunk.GetChunkIndex()),
			streamTotalChunksAppProperty:   int64(chunk.GetTotalChunks()),
			streamIsFinalAppProperty:       chunk.GetIsFinal(),
		}
		publishingID := atomic.AddInt64(&s.streamPublishSeq, 1)
		message.SetPublishingId(publishingID)
		publishingIDs = append(publishingIDs, publishingID)
		messages = append(messages, message)
	}

	// Register the publishing IDs BEFORE sending so the watcher cannot signal a
	// confirmation we never registered (which would result in deadlock waiting on
	// the done channel).
	done := s.registerStreamPublish(publishingIDs)

	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			s.cancelStreamPublish(publishingIDs)
			return &emptypb.Empty{}, err
		}
		if err := producer.Send(message); err != nil {
			s.cancelStreamPublish(publishingIDs)
			return &emptypb.Empty{}, fmt.Errorf("enqueue stream chunk for %s/%s to %q: %w", correlationID, messageID, streamName, err)
		}
	}

	timeout := time.Duration(envInt("RABBITMQ_STREAM_PUBLISH_TIMEOUT_SECS", defaultStreamPublishTimeoutSecs)) * time.Second
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case publishErr := <-done:
		if publishErr != nil {
			return &emptypb.Empty{}, fmt.Errorf("rabbitmq stream publish failed for %s/%s to %q: %w", correlationID, messageID, streamName, publishErr)
		}
		logger.Sugar().Infow(
			"Confirmed microservice communication chunks through RabbitMQ Streams",
			"stream", streamName,
			"correlation_id", correlationID,
			"message_id", messageID,
			"chunks", len(chunks),
		)
		return &emptypb.Empty{}, nil
	case <-ctx.Done():
		s.cancelStreamPublish(publishingIDs)
		return &emptypb.Empty{}, ctx.Err()
	case <-timer.C:
		s.cancelStreamPublish(publishingIDs)
		return &emptypb.Empty{}, fmt.Errorf("timed out waiting for rabbitmq stream publish confirmation for %s/%s to %q", correlationID, messageID, streamName)
	}
}

func (s *serverInstance) consumeRabbitMQStream(ctx context.Context, queueName string, grpcStream pb.RabbitMQ_ConsumeServer) {
	if !isAgentIngressQueue(queueName) {
		return
	}

	env, err := openRabbitMQStreamEnvironment()
	if err != nil {
		logger.Sugar().Warnf("RabbitMQ stream consumer disabled for %s: %v", queueName, err)
		return
	}

	streamName := rabbitStreamName(queueName)
	if err := declareRabbitMQStream(env, streamName); err != nil {
		logger.Sugar().Warnf("RabbitMQ stream consumer disabled for %s: %v", queueName, err)
		_ = env.Close()
		return
	}

	buffers := make(map[string]*microserviceChunkAccumulator)
	bufferMutex := &sync.Mutex{}
	consumerName := fmt.Sprintf("%s-sidecar", streamName)
	offsetSpec, err := resolveRabbitMQStreamOffset(env, consumerName, streamName)
	if err != nil {
		logger.Sugar().Warnf("RabbitMQ stream consumer disabled for %s: %v", queueName, err)
		_ = env.Close()
		return
	}

	consumer, err := env.NewConsumer(
		streamName,
		func(consumerContext stream.ConsumerContext, message *streamamqp.Message) {
			offset := consumerContext.Consumer.GetOffset()
			msComm, complete, safeToCommit, err := handleRabbitMQStreamChunkAtOffset(message, offset, buffers, bufferMutex)
			if err != nil {
				logger.Sugar().Errorf("Error handling RabbitMQ stream chunk: %v", err)
				return
			}
			if !complete {
				return
			}
			logger.Sugar().Debugw("Reassembled RabbitMQ stream microservice communication", "stream", streamName, "correlation_id", msComm.GetRequestMetadata().GetCorrelationId())

			anyMsg, err := anypb.New(msComm)
			if err != nil {
				logger.Sugar().Errorf("Error packing reassembled stream message: %v", err)
				return
			}

			grpcMsg := &pb.SideCarMessage{
				Type:   "microserviceCommunication",
				Body:   anyMsg,
				Traces: msComm.Traces,
			}

			sendMutex.Lock()
			err = grpcStream.SendMsg(grpcMsg)
			sendMutex.Unlock()
			if err != nil {
				logger.Sugar().Warnf("stream error: %v", err)
				return
			}
			if !safeToCommit {
				logger.Sugar().Debugw(
					"Deferring RabbitMQ stream offset commit while incomplete interleaved chunks remain",
					"stream", streamName,
					"offset", offset,
					"correlation_id", msComm.GetRequestMetadata().GetCorrelationId(),
				)
				return
			}
			if err := consumerContext.Consumer.StoreCustomOffset(offset); err != nil {
				logger.Sugar().Warnw("Failed to store RabbitMQ stream offset after forwarding message", "stream", streamName, "offset", offset, "error", err)
			}
		},
		stream.NewConsumerOptions().
			SetConsumerName(consumerName).
			SetCRCCheck(false).
			SetInitialCredits(streamInitialCredits).
			SetManualCommit().
			SetOffset(offsetSpec),
	)
	if err != nil {
		logger.Sugar().Warnf("RabbitMQ stream consumer disabled for %s: %v", queueName, err)
		_ = env.Close()
		return
	}

	logger.Sugar().Infof("Started consuming RabbitMQ stream %s", streamName)
	<-ctx.Done()
	_ = consumer.Close()
	_ = env.Close()
}

func resolveRabbitMQStreamOffset(env *stream.Environment, consumerName string, streamName string) (stream.OffsetSpecification, error) {
	offset, err := env.QueryOffset(consumerName, streamName)
	if err != nil {
		if errors.Is(err, stream.OffsetNotFoundError) {
			return stream.OffsetSpecification{}.Next(), nil
		}
		return stream.OffsetSpecification{}, fmt.Errorf("query offset for consumer %q on %q: %w", consumerName, streamName, err)
	}
	return stream.OffsetSpecification{}.Offset(offset + 1), nil
}

func handleRabbitMQStreamChunk(message *streamamqp.Message, buffers map[string]*microserviceChunkAccumulator, bufferMutex *sync.Mutex) (*pb.MicroserviceCommunication, bool, error) {
	msComm, complete, _, err := handleRabbitMQStreamChunkAtOffset(message, -1, buffers, bufferMutex)
	return msComm, complete, err
}

func handleRabbitMQStreamChunkAtOffset(message *streamamqp.Message, offset int64, buffers map[string]*microserviceChunkAccumulator, bufferMutex *sync.Mutex) (*pb.MicroserviceCommunication, bool, bool, error) {
	if message == nil || len(message.GetData()) == 0 {
		return nil, false, false, errors.New("empty RabbitMQ stream message")
	}

	chunk := &pb.MicroserviceCommunicationChunk{}
	if err := proto.Unmarshal(message.GetData(), chunk); err != nil {
		return nil, false, false, fmt.Errorf("unmarshal stream chunk: %w", err)
	}

	correlationID := chunk.GetCorrelationId()
	if correlationID == "" && chunk.GetRequestMetadata() != nil {
		correlationID = chunk.GetRequestMetadata().GetCorrelationId()
	}
	if correlationID == "" {
		return nil, false, false, errors.New("stream chunk missing correlation id")
	}

	bufferMutex.Lock()
	messageID := streamMessageIDFromAMQPMessage(message)
	bufferKey := streamAccumulatorKey(correlationID, messageID)
	acc, ok := buffers[bufferKey]
	if !ok {
		acc = &microserviceChunkAccumulator{
			chunks:  make(map[uint32]*pb.MicroserviceCommunicationChunk),
			offsets: make(map[uint32]int64),
		}
		buffers[bufferKey] = acc
	}
	acc.correlationID = correlationID
	acc.messageID = messageID
	acc.chunks[chunk.GetChunkIndex()] = chunk
	if offset >= 0 {
		acc.offsets[chunk.GetChunkIndex()] = offset
	}
	if chunk.GetIsFinal() {
		acc.finalSeen = true
		acc.totalChunks = chunk.GetTotalChunks()
		if acc.totalChunks == 0 {
			acc.totalChunks = chunk.GetChunkIndex() + 1
		}
	}

	complete := acc.isComplete()
	var msComm *pb.MicroserviceCommunication
	var err error
	safeToCommit := false
	if complete {
		msComm, err = acc.reassemble()
		delete(buffers, bufferKey)
		safeToCommit = len(buffers) == 0
	}
	bufferMutex.Unlock()

	if err != nil {
		return nil, false, false, err
	}
	return msComm, complete, safeToCommit, nil
}

func streamAccumulatorKey(correlationID string, messageID string) string {
	if messageID == "" {
		return correlationID
	}
	return correlationID + "/" + messageID
}

func (a *microserviceChunkAccumulator) isComplete() bool {
	if a == nil || !a.finalSeen || a.totalChunks == 0 {
		return false
	}
	for index := uint32(0); index < a.totalChunks; index++ {
		if _, ok := a.chunks[index]; !ok {
			return false
		}
	}
	return true
}

func (a *microserviceChunkAccumulator) reassemble() (*pb.MicroserviceCommunication, error) {
	if a == nil || len(a.chunks) == 0 {
		return nil, errors.New("no stream chunks to reassemble")
	}

	ordered := make([]int, 0, len(a.chunks))
	for index := range a.chunks {
		ordered = append(ordered, int(index))
	}
	sort.Ints(ordered)

	first := a.chunks[uint32(ordered[0])]
	msComm := &pb.MicroserviceCommunication{
		Type:            first.GetType(),
		RequestType:     first.GetRequestType(),
		Metadata:        cloneStringMap(first.GetMetadata()),
		OriginalRequest: first.GetOriginalRequest(),
		RequestMetadata: first.GetRequestMetadata(),
		Traces:          cloneBytesMap(first.GetTraces()),
		RoutingData:     append([]string(nil), first.GetRoutingData()...),
	}

	mergedData := &structpb.Struct{Fields: make(map[string]*structpb.Value)}
	hasData := false
	for _, index := range ordered {
		chunk := a.chunks[uint32(index)]
		if len(chunk.GetResultChunk()) > 0 {
			msComm.Result = append(msComm.Result, chunk.GetResultChunk()...)
		}
		if chunk.GetData() != nil && len(chunk.GetData().GetFields()) > 0 {
			hasData = true
			mergeStructData(mergedData, chunk.GetData())
		}
	}
	if hasData {
		msComm.Data = mergedData
	}
	if msComm.RequestMetadata != nil {
		msComm.RequestMetadata.Transport = lib.TransportRabbitMQStreams
	}
	if msComm.Metadata == nil {
		msComm.Metadata = map[string]string{}
	}
	msComm.Metadata[lib.TransportMetadataKey] = lib.TransportRabbitMQStreams

	return msComm, nil
}

func splitMicroserviceCommunication(data *pb.MicroserviceCommunication) ([]*pb.MicroserviceCommunicationChunk, error) {
	if data == nil || data.RequestMetadata == nil {
		return nil, errors.New("microservice communication missing request metadata")
	}

	rowsPerChunk, bytesPerChunk := streamChunkSettings()
	chunks := make([]*pb.MicroserviceCommunicationChunk, 0)

	for _, dataChunk := range splitStructByRowBatches(data.GetData(), rowsPerChunk, bytesPerChunk) {
		chunks = append(chunks, newMicroserviceCommunicationChunk(data, dataChunk, nil))
	}

	for _, resultChunk := range splitBytes(data.GetResult(), bytesPerChunk) {
		chunks = append(chunks, newMicroserviceCommunicationChunk(data, nil, resultChunk))
	}

	if len(chunks) == 0 {
		chunks = append(chunks, newMicroserviceCommunicationChunk(data, nil, nil))
	}

	totalChunks := uint32(len(chunks))
	for index, chunk := range chunks {
		chunk.ChunkIndex = uint32(index)
		chunk.TotalChunks = totalChunks
		chunk.IsFinal = uint32(index) == totalChunks-1
	}
	return chunks, nil
}

func newMicroserviceCommunicationChunk(data *pb.MicroserviceCommunication, chunkData *structpb.Struct, resultChunk []byte) *pb.MicroserviceCommunicationChunk {
	metadata := cloneStringMap(data.GetMetadata())
	if metadata == nil {
		metadata = map[string]string{}
	}
	transport := resolveMicroserviceTransport(data)
	metadata[lib.TransportMetadataKey] = transport
	if data.RequestMetadata != nil {
		data.RequestMetadata.Transport = transport
	}

	return &pb.MicroserviceCommunicationChunk{
		Type:            data.GetType(),
		RequestType:     data.GetRequestType(),
		Data:            chunkData,
		Metadata:        metadata,
		OriginalRequest: data.GetOriginalRequest(),
		RequestMetadata: data.GetRequestMetadata(),
		Traces:          cloneBytesMap(data.GetTraces()),
		ResultChunk:     append([]byte(nil), resultChunk...),
		RoutingData:     append([]string(nil), data.GetRoutingData()...),
		CorrelationId:   data.GetRequestMetadata().GetCorrelationId(),
	}
}

func splitStructByRowBatches(data *structpb.Struct, rowsPerChunk int, bytesPerChunk int) []*structpb.Struct {
	if data == nil || len(data.GetFields()) == 0 {
		return nil
	}

	rowCount := structRowCount(data)
	if rowCount == 0 {
		return []*structpb.Struct{data}
	}

	chunks := make([]*structpb.Struct, 0, (rowCount/rowsPerChunk)+1)
	for start := 0; start < rowCount; {
		end := min(start+rowsPerChunk, rowCount)
		for {
			candidate := sliceStructRows(data, start, end)
			if end-start == 1 || estimatedStructSize(candidate) <= bytesPerChunk {
				chunks = append(chunks, candidate)
				start = end
				break
			}
			end = start + max(1, (end-start)/2)
		}
	}
	return chunks
}

func splitBytes(data []byte, bytesPerChunk int) [][]byte {
	if len(data) == 0 {
		return nil
	}
	chunks := make([][]byte, 0, (len(data)/bytesPerChunk)+1)
	for start := 0; start < len(data); start += bytesPerChunk {
		end := min(start+bytesPerChunk, len(data))
		chunks = append(chunks, append([]byte(nil), data[start:end]...))
	}
	return chunks
}

func streamChunkSettings() (int, int) {
	rows := envInt("RABBITMQ_STREAM_CHUNK_ROWS", defaultStreamChunkRows)
	bytes := envInt("RABBITMQ_STREAM_CHUNK_BYTES", defaultStreamChunkBytes)
	if rows <= 0 {
		rows = defaultStreamChunkRows
	}
	if bytes <= 0 {
		bytes = defaultStreamChunkBytes
	}
	return rows, bytes
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func appString(values map[string]interface{}, key string) string {
	if values == nil {
		return ""
	}
	value, ok := values[key]
	if !ok || value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	return fmt.Sprint(value)
}

func streamMessageIDFromStreamMessage(message messagepkg.StreamMessage) string {
	if message == nil {
		return ""
	}
	if messageID := appString(message.GetApplicationProperties(), streamMessageIDAppProperty); messageID != "" {
		return messageID
	}
	return streamMessageIDFromProperties(message.GetMessageProperties())
}

func streamMessageIDFromAMQPMessage(message *streamamqp.Message) string {
	if message == nil {
		return ""
	}

	if messageID := appString(message.ApplicationProperties, streamMessageIDAppProperty); messageID != "" {
		return messageID
	}
	return streamMessageIDFromProperties(message.Properties)
}

func streamMessageIDFromProperties(properties *streamamqp.MessageProperties) string {
	if properties == nil || properties.CorrelationID == nil {
		return ""
	}

	switch value := properties.CorrelationID.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}

func structRowCount(data *structpb.Struct) int {
	maxRows := 0
	for _, value := range data.GetFields() {
		if list := value.GetListValue(); list != nil && len(list.GetValues()) > maxRows {
			maxRows = len(list.GetValues())
		}
	}
	return maxRows
}

func sliceStructRows(data *structpb.Struct, start int, end int) *structpb.Struct {
	fields := make(map[string]*structpb.Value, len(data.GetFields()))
	for key, value := range data.GetFields() {
		list := value.GetListValue()
		if list == nil {
			fields[key] = value
			continue
		}

		values := list.GetValues()
		if start >= len(values) {
			fields[key] = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{}})
			continue
		}
		fieldEnd := min(end, len(values))
		fields[key] = structpb.NewListValue(&structpb.ListValue{Values: append([]*structpb.Value(nil), values[start:fieldEnd]...)})
	}
	return &structpb.Struct{Fields: fields}
}

func estimatedStructSize(data *structpb.Struct) int {
	body, err := proto.Marshal(data)
	if err != nil {
		return defaultStreamChunkBytes + 1
	}
	return len(body)
}

func mergeStructData(target *structpb.Struct, source *structpb.Struct) {
	for key, sourceValue := range source.GetFields() {
		sourceList := sourceValue.GetListValue()
		if sourceList == nil {
			if _, exists := target.Fields[key]; !exists {
				target.Fields[key] = sourceValue
			}
			continue
		}

		existing, exists := target.Fields[key]
		if !exists || existing.GetListValue() == nil {
			target.Fields[key] = structpb.NewListValue(&structpb.ListValue{Values: append([]*structpb.Value(nil), sourceList.GetValues()...)})
			continue
		}

		merged := append(existing.GetListValue().GetValues(), sourceList.GetValues()...)
		target.Fields[key] = structpb.NewListValue(&structpb.ListValue{Values: merged})
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneBytesMap(input map[string][]byte) map[string][]byte {
	if input == nil {
		return nil
	}
	output := make(map[string][]byte, len(input))
	for key, value := range input {
		output[key] = append([]byte(nil), value...)
	}
	return output
}

func (s *serverInstance) SendMicroserviceCommStream(streamServer pb.RabbitMQ_SendMicroserviceCommStreamServer) error {
	var chunks []*pb.MicroserviceCommunicationChunk
	for {
		chunk, err := streamServer.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return status.Error(codes.Canceled, err.Error())
			}
			if errors.Is(err, io.EOF) {
				break
			}
			return status.Error(codes.Internal, err.Error())
		}
		chunks = append(chunks, chunk)
		if chunk.GetIsFinal() {
			break
		}
	}

	acc := &microserviceChunkAccumulator{chunks: make(map[uint32]*pb.MicroserviceCommunicationChunk)}
	for _, chunk := range chunks {
		acc.chunks[chunk.GetChunkIndex()] = chunk
		if chunk.GetIsFinal() {
			acc.finalSeen = true
			acc.totalChunks = chunk.GetTotalChunks()
		}
	}
	msComm, err := acc.reassemble()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	_, err = SendDataThroughRabbitMQStream(streamServer.Context(), msComm, msComm.GetRequestMetadata().GetDestinationQueue(), s)
	return err
}
