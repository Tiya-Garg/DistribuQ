package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func registerWebhookSubscription(addr, targetURL, secret string, timeout time.Duration) {
	host := addr
	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		if parts[1] == "8000" {
			host = parts[0] + ":8080"
		}
	}
	url := fmt.Sprintf("http://%s/api/webhooks", host)

	payload := map[string]any{
		"url":    targetURL,
		"secret": secret,
		"events": []string{"task.completed", "task.failed", "task.dlq"},
	}
	body, _ := json.Marshal(payload)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()
}
