package lib

import (
	"strconv"
	"strings"
)

const (
	TransportUnary                 = "unary"
	TransportStreaming             = "streaming"
	TransportRabbitMQStreams       = "rabbitmq-streams"
	TransportMetadataKey           = "transport"
	StreamPartialMetadataKey       = "stream_partial"
	StreamFinalMetadataKey         = "stream_final"
	StreamSequenceMetadataKey      = "stream_sequence"
	StreamRowsProcessedMetadataKey = "stream_rows_processed"
	StreamRowsTotalMetadataKey     = "stream_rows_total"
	StreamProviderMetadataKey      = "stream_provider"
	StreamBatchIDMetadataKey       = "stream_batch_id"
)

func NormalizeTransport(transport string) string {
	switch strings.ToLower(strings.TrimSpace(transport)) {
	case TransportStreaming:
		return TransportStreaming
	case TransportRabbitMQStreams:
		return TransportRabbitMQStreams
	default:
		return TransportUnary
	}
}

func IsStreamingTransport(transport string) bool {
	normalizedTransport := NormalizeTransport(transport)
	return normalizedTransport == TransportStreaming || normalizedTransport == TransportRabbitMQStreams
}

func IsGrpcStreamingTransport(transport string) bool {
	return NormalizeTransport(transport) == TransportStreaming
}

func IsRabbitMQStreamingTransport(transport string) bool {
	return NormalizeTransport(transport) == TransportRabbitMQStreams
}

func MetadataBool(metadata map[string]string, key string, defaultValue bool) bool {
	if metadata == nil {
		return defaultValue
	}

	value, ok := metadata[key]
	if !ok {
		return defaultValue
	}

	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes":
		return true
	case "0", "false", "no":
		return false
	default:
		return defaultValue
	}
}

func MetadataInt(metadata map[string]string, key string) int {
	if metadata == nil {
		return 0
	}

	value := strings.TrimSpace(metadata[key])
	if value == "" {
		return 0
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}

	return parsed
}
