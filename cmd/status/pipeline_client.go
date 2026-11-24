package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func queryPipelines(addr, pipelineID, token string, timeout time.Duration) error {
	host := addr
	if strings.Contains(host, ":") {
		parts := strings.Split(host, ":")
		if parts[1] == "8000" {
			host = parts[0] + ":8080"
		}
	}

	url := fmt.Sprintf("http://%s/api/pipelines", host)
	if pipelineID != "" {
		url = fmt.Sprintf("%s/%s", url, pipelineID)
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to query pipelines from %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("query pipelines failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err != nil {
		fmt.Println(string(body))
	} else {
		fmt.Println(pretty.String())
	}

	return nil
}
