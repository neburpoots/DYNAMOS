package msinit

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"contrib.go.opencensus.io/exporter/ocagent"
	"github.com/Jorrit05/DYNAMOS/pkg/lib"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	logger = lib.InitLogger(zap.DebugLevel)
)

func (s *Configuration) CloseConnection() {
	s.closeNextClientStreams()

	if s.NextClientConnection != nil {
		s.NextClientConnection.Close()
	}

	if s.RabbitMsgClientConnection != nil {
		s.RabbitMsgClientConnection.Close()
	}
}

type Configuration struct {
	Port         uint32
	FirstService bool
	LastService  bool
	ServiceName  string

	MessageHandler   func(conf *Configuration) func(ctx context.Context, data *pb.MicroserviceCommunication) error
	StopMicroservice chan struct{} // channel to continue the main routine to kill the MS

	GrpcServer                *grpc.Server
	RabbitMsgClientConnection *grpc.ClientConn
	NextClientConnection      *grpc.ClientConn
	RabbitMsgClient           pb.RabbitMQClient
	NextClient                pb.MicroserviceClient
	nextClientStreamMu        sync.Mutex
	nextClientStreams         map[string]pb.Microservice_SendDataStreamClient
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

func isFinalStreamMessage(data *pb.MicroserviceCommunication) bool {
	partial := lib.MetadataBool(data.GetMetadata(), lib.StreamPartialMetadataKey, false)
	return lib.MetadataBool(data.GetMetadata(), lib.StreamFinalMetadataKey, !partial)
}

func (s *Configuration) closeNextClientStreams() {
	s.nextClientStreamMu.Lock()
	defer s.nextClientStreamMu.Unlock()

	for correlationID, stream := range s.nextClientStreams {
		if stream != nil {
			_ = stream.CloseSend()
		}
		delete(s.nextClientStreams, correlationID)
	}
}

func (s *Configuration) getOrCreateNextClientStream(ctx context.Context, correlationID string) (pb.Microservice_SendDataStreamClient, error) {
	s.nextClientStreamMu.Lock()
	defer s.nextClientStreamMu.Unlock()

	if s.nextClientStreams == nil {
		s.nextClientStreams = make(map[string]pb.Microservice_SendDataStreamClient)
	}

	if stream, ok := s.nextClientStreams[correlationID]; ok {
		return stream, nil
	}

	// Outbound streams can span multiple inbound handler invocations for the same
	// correlation ID, so they must not inherit a single request-scoped context.
	stream, err := s.NextClient.SendDataStream(context.Background())
	if err != nil {
		return nil, err
	}

	s.nextClientStreams[correlationID] = stream
	return stream, nil
}

func (s *Configuration) dropNextClientStream(correlationID string, closeStream bool) {
	s.nextClientStreamMu.Lock()
	stream := s.nextClientStreams[correlationID]
	delete(s.nextClientStreams, correlationID)
	s.nextClientStreamMu.Unlock()

	if closeStream && stream != nil {
		_ = stream.CloseSend()
	}
}

func (s *Configuration) SendToNext(ctx context.Context, data *pb.MicroserviceCommunication) error {
	if s.NextClient == nil {
		return fmt.Errorf("next client is not configured")
	}

	transport := resolveMicroserviceTransport(data)
	if !lib.IsGrpcStreamingTransport(transport) {
		_, err := s.NextClient.SendData(ctx, data)
		return err
	}

	correlationID := ""
	if data.RequestMetadata != nil {
		correlationID = data.RequestMetadata.CorrelationId
	}
	if correlationID == "" {
		_, err := s.NextClient.SendData(ctx, data)
		return err
	}

	stream, err := s.getOrCreateNextClientStream(ctx, correlationID)
	if err != nil {
		return err
	}

	if err := stream.Send(data); err != nil {
		s.dropNextClientStream(correlationID, true)
		return err
	}

	if !isFinalStreamMessage(data) {
		return nil
	}

	_, err = stream.CloseAndRecv()
	s.dropNextClientStream(correlationID, false)
	return err
}

func NewConfiguration(
	ctx context.Context,
	serviceName string,
	grpcAddr string,
	COORDINATOR chan struct{},
	messageHandler func(conf *Configuration) func(ctx context.Context, data *pb.MicroserviceCommunication) error,
) (*Configuration, error) {

	port, err := strconv.Atoi(os.Getenv("DESIGNATED_GRPC_PORT"))
	if err != nil {
		return nil, fmt.Errorf("error determining port number: %w", err)
	}
	firstService, err := strconv.Atoi(os.Getenv("FIRST"))
	if err != nil {
		return nil, fmt.Errorf("error determining first service: %w", err)
	}

	lastService, err := strconv.Atoi(os.Getenv("LAST"))
	if err != nil {
		return nil, fmt.Errorf("error determining last service: %w", err)
	}

	jobName := os.Getenv("JOB_NAME")
	if jobName == "" {
		logger.Sugar().Fatalf("Jobname not defined.")
	}

	logger.Sugar().Debugf("NewConfiguration %s, firstServer: %s, port: %s. lastservice: %s", serviceName, firstService, port, lastService)

	// Create a new configuration instance with the provided parameters
	conf := &Configuration{
		Port:                      uint32(port),
		FirstService:              firstService > 0,
		LastService:               lastService > 0,
		ServiceName:               serviceName,
		RabbitMsgClient:           nil,
		RabbitMsgClientConnection: nil,
		NextClientConnection:      nil,
		NextClient:                nil,
		MessageHandler:            messageHandler,
		StopMicroservice:          make(chan struct{}), // Continue the main routine to kill the MS
		GrpcServer:                nil,
	}

	if conf.FirstService {
		conf.GrpcServer = grpc.NewServer()
		conf.StartGrpcServer()
		conf.RabbitMsgClientConnection = lib.GetGrpcConnection(grpcAddr + os.Getenv("SIDECAR_PORT"))
		conf.RabbitMsgClient = pb.NewRabbitMQClient(conf.RabbitMsgClientConnection)

		if conf.LastService {
			conf.NextClientConnection = lib.GetGrpcConnection(grpcAddr + os.Getenv("SIDECAR_PORT"))
			conf.NextClient = pb.NewMicroserviceClient(conf.NextClientConnection)
		} else {
			conf.NextClientConnection = lib.GetGrpcConnection(grpcAddr + strconv.Itoa(int(conf.Port)+1))
			conf.NextClient = pb.NewMicroserviceClient(conf.NextClientConnection)
		}

		// When being called from an MS, QueueAutoDelete should be false, queues
		// are managed in the compositionRequest handler.
		chainRequest := &pb.ChainRequest{
			ServiceName:     conf.ServiceName,
			RoutingKey:      jobName,
			QueueAutoDelete: false,
			Port:            conf.Port,
		}

		var initErr error
		for attempt := 1; attempt <= 7; attempt++ {
			initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, initErr = conf.RabbitMsgClient.InitRabbitForChain(initCtx, chainRequest)
			cancel()
			if initErr == nil {
				break
			}

			logger.Sugar().Warnw("InitRabbitForChain failed", "service", conf.ServiceName, "routingKey", chainRequest.RoutingKey, "attempt", attempt, "error", initErr)
			if attempt < 7 {
				time.Sleep(1 * time.Second)
			}
		}
		if initErr != nil {
			return nil, fmt.Errorf("failed to init rabbit chain for %s: %w", conf.ServiceName, initErr)
		}

	} else if conf.LastService {
		conf.GrpcServer = grpc.NewServer()
		conf.StartGrpcServer()
		conf.NextClientConnection = lib.GetGrpcConnection(grpcAddr + os.Getenv("SIDECAR_PORT"))
		conf.NextClient = pb.NewMicroserviceClient(conf.NextClientConnection)
		conf.RabbitMsgClientConnection = conf.NextClientConnection
		conf.RabbitMsgClient = pb.NewRabbitMQClient(conf.RabbitMsgClientConnection)
	} else {
		conf.GrpcServer = grpc.NewServer()
		conf.StartGrpcServer()
		conf.NextClientConnection = lib.GetGrpcConnection(grpcAddr + strconv.Itoa(int(conf.Port)+1))
		conf.NextClient = pb.NewMicroserviceClient(conf.NextClientConnection)
	}

	close(COORDINATOR)
	return conf, nil
}

// Register a gRPC server on our designated port
// StartGrpcServer starts the gRPC server for the Configuration instance.
// It listens on the specified port and registers the MicroserviceServer and HealthServer
// with the gRPC server. It also sets up this server with a callback from the initiating service.
//
// The server is started in a separate goroutine.
// parameters:
// - none
//
// returns:
// - none
func (s *Configuration) StartGrpcServer() {

	go func() {
		logger.Sugar().Infof("Start listening on port: %v", s.Port)
		lis, err := net.Listen("tcp", fmt.Sprintf(":%v", s.Port))
		if err != nil {
			logger.Sugar().Fatalw("failed to listen: %v", err)
		}
		serverInstance := &lib.SharedServer{ServiceName: s.ServiceName, Callback: s.MessageHandler(s)}

		pb.RegisterMicroserviceServer(s.GrpcServer, serverInstance)
		pb.RegisterHealthServer(s.GrpcServer, serverInstance)

		if err := s.GrpcServer.Serve(lis); err != nil {
			logger.Sugar().Fatalw("failed to serve: %v", err)
		}
	}()
}

func (s *Configuration) SafeExit(oce *ocagent.Exporter, serviceName string) {
	logger.Debug("Start SafeExit")

	if s.LastService {
		if s.RabbitMsgClient == nil {
			logger.Sugar().Error("RabbitMsgClient is nil while we should send a StopReceivingRabbit signal")
		} else {
			logger.Sugar().Debugw("Send StopReceivingRabbit", "service", serviceName)
			_, err := s.RabbitMsgClient.StopReceivingRabbit(context.Background(), &pb.StopRequest{})
			if err != nil {
				logger.Sugar().Errorf("Error stopping receiving rabbit: %v", err)
			}
		}
	}

	logger.Sugar().Infof("Wait 2 seconds before ending %s", serviceName)
	oce.Flush()
	time.Sleep(2 * time.Second)
	oce.Stop()
	logger.Sugar().Debug("Start closing gRPC connections NextClientConnection and RabbitConnection")

	s.CloseConnection()

	if s.GrpcServer != nil {
		logger.Sugar().Debug("Close own gRPC server")
		s.StopGrpcServer()
	}
}

func (s *Configuration) StopGrpcServer() {
	logger.Info("Stopping StopGrpcServer")
	timeout := time.After(5 * time.Second)
	done := make(chan bool)

	go func() {
		s.GrpcServer.GracefulStop()
		done <- true
	}()

	select {
	case <-timeout:
		logger.Info("Hard stop")
		s.GrpcServer.Stop() // forcefully stop if graceful stop did not complete within timeout
	case <-done:
		logger.Info("Finished graceful stop")
	}
}
