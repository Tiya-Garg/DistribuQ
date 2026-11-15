package worker

import (
	"strings"
	"testing"
	"time"
)

func TestShellExecutorWithStreamerCapturesOutput(t *testing.T) {
	var capturedOutput string
	var capturedStderr bool
	streamer := func(taskID, workerID, content string, isStderr bool) {
		capturedOutput = content
		capturedStderr = isStderr
	}

	exec := NewShellExecutorWithStreamer("w-test", 5*time.Second, streamer)
	err := exec("t-stream", "echo streaming works")
	if err != nil {
		t.Fatalf("unexpected exec error: %v", err)
	}

	if !strings.Contains(capturedOutput, "streaming works") {
		t.Fatalf("expected output to contain 'streaming works', got %q", capturedOutput)
	}
	if capturedStderr {
		t.Fatalf("expected isStderr to be false for successful echo")
	}
}
