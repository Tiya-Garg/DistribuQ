package webhook

import (
	"encoding/json"
	"fmt"
)

// SlackPayload represents an incoming webhook message compatible with Slack/Discord.
type SlackPayload struct {
	Text        string            `json:"text"`
	Attachments []SlackAttachment `json:"attachments,omitempty"`
}

// SlackAttachment defines an enriched attachment block.
type SlackAttachment struct {
	Color  string       `json:"color"`
	Title  string       `json:"title"`
	Fields []SlackField `json:"fields,omitempty"`
	Footer string       `json:"footer,omitempty"`
}

// SlackField defines key-value metadata within an attachment.
type SlackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// FormatSlack transforms an internal cluster event into a Slack-compatible JSON message.
func FormatSlack(event Event) ([]byte, error) {
	color := "#36a64f" // green
	if event.Type == EventTaskFailed || event.Type == EventTaskDLQ || event.Type == EventWorkerOffline {
		color = "#dc3545" // red
	} else if event.Type == EventAutoscale {
		color = "#ffc107" // amber
	}

	title := fmt.Sprintf("DistribuQ Event: %s", event.Type)
	var fields []SlackField
	fields = append(fields, SlackField{Title: "Event ID", Value: event.ID, Short: true})
	fields = append(fields, SlackField{Title: "Time", Value: event.Timestamp.Format("15:04:05 MST"), Short: true})

	for k, v := range event.Payload {
		fields = append(fields, SlackField{Title: k, Value: fmt.Sprintf("%v", v), Short: true})
	}

	msg := SlackPayload{
		Text: fmt.Sprintf("[%s] Cluster notification: %s", event.Type, event.ID),
		Attachments: []SlackAttachment{
			{
				Color:  color,
				Title:  title,
				Fields: fields,
				Footer: "DistribuQ Webhook Dispatcher",
			},
		},
	}

	return json.Marshal(msg)
}
