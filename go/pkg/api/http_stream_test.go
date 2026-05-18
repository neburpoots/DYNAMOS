package api

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWantsNDJSON(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept", "application/x-ndjson, application/json")
	if !WantsNDJSON(req) {
		t.Fatalf("expected WantsNDJSON to detect application/x-ndjson")
	}
}

func TestStreamResponseSetResultBodyJSON(t *testing.T) {
	event := StreamResponse{}
	event.SetResultBody([]byte(`{"ok":true}`))
	if string(event.Result) != `{"ok":true}` {
		t.Fatalf("expected JSON result body to be preserved, got %s", string(event.Result))
	}
	if event.ResultText != "" {
		t.Fatalf("expected ResultText to be empty for JSON payload, got %q", event.ResultText)
	}
}

func TestStreamResponseSetResultBodyText(t *testing.T) {
	event := StreamResponse{}
	event.SetResultBody([]byte("plain text result"))
	if string(event.Result) != "" {
		t.Fatalf("expected Result to be empty for text payload, got %s", string(event.Result))
	}
	if event.ResultText != "plain text result" {
		t.Fatalf("expected ResultText to preserve the text payload, got %q", event.ResultText)
	}
}

func TestWriteNDJSON(t *testing.T) {
	var buffer bytes.Buffer
	err := WriteNDJSON(&buffer, StreamResponse{Type: StreamEventTypeDone, JobID: "job-1"})
	if err != nil {
		t.Fatalf("WriteNDJSON returned error: %v", err)
	}
	if !strings.HasSuffix(buffer.String(), "\n") {
		t.Fatalf("expected NDJSON payload to end with a newline, got %q", buffer.String())
	}
}