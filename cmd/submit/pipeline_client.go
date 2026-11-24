package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func submitPipelineFile(addr, filePath, token string, timeout time.Duration) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read pipeline file: %w", err)
	}

	host := addr
	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		if parts[1] == "8000" {
			host = parts[0] + ":8080"
		}
	}
	url := fmt.Sprintf("http://%s/api/pipelines", host)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to submit pipeline to %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("pipeline submission failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	fmt.Printf("pipeline submitted successfully: %s\n", string(body))
	return nil
}
