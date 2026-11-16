package scheduler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tiya-Garg/DistribuQ/internal/webhook"
)

func TestHandleWebhooksHTTPCRUD(t *testing.T) {
	store := webhook.NewStore()
	dispatcher := webhook.NewDispatcher(store, nil)
	s := NewScheduler(nil)
	s.SetWebhookDispatcher(dispatcher)

	// 1. GET empty
	req := httptest.NewRequest(http.MethodGet, "/api/webhooks", nil)
	w := httptest.NewRecorder()
	s.HandleWebhooksHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// 2. POST create subscription
	body := []byte(`{"id":"sub-alert","url":"https://alert.io/hook","secret":"sec"}`)
	postReq := httptest.NewRequest(http.MethodPost, "/api/webhooks", bytes.NewReader(body))
	wPost := httptest.NewRecorder()
	s.HandleWebhooksHTTP(wPost, postReq)
	if wPost.Code != http.StatusOK {
		t.Fatalf("expected 200 on create, got %d", wPost.Code)
	}

	// 3. GET verify created
	wList := httptest.NewRecorder()
	s.HandleWebhooksHTTP(wList, req)
	var listResp struct {
		Enabled       bool                   `json:"enabled"`
		Subscriptions []webhook.Subscription `json:"subscriptions"`
	}
	_ = json.Unmarshal(wList.Body.Bytes(), &listResp)
	if len(listResp.Subscriptions) != 1 || listResp.Subscriptions[0].ID != "sub-alert" {
		t.Fatalf("expected 1 subscription, got %+v", listResp)
	}

	// 4. DELETE subscription
	delReq := httptest.NewRequest(http.MethodDelete, "/api/webhooks?id=sub-alert", nil)
	wDel := httptest.NewRecorder()
	s.HandleWebhooksHTTP(wDel, delReq)
	if wDel.Code != http.StatusOK {
		t.Fatalf("expected 200 on delete, got %d", wDel.Code)
	}
}
