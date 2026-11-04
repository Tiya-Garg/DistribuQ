package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Tiya-Garg/DistribuQ/internal/raft"
	"github.com/Tiya-Garg/DistribuQ/internal/scheduler"
)

//go:embed static/*
var staticFS embed.FS

// ClusterStateView aggregates full cluster telemetry for dashboard views.
type ClusterStateView struct {
	Raft    RaftView               `json:"raft"`
	Workers []scheduler.WorkerView `json:"workers"`
	Tasks   []scheduler.TaskView   `json:"tasks"`
}

type RaftView struct {
	NodeID            string   `json:"node_id"`
	Role              string   `json:"role"`
	Term              int      `json:"term"`
	LeaderID          string   `json:"leader_id"`
	CommitIndex       int      `json:"commit_index"`
	LastLogIndex      int      `json:"last_log_index"`
	LastIncludedIndex int      `json:"last_included_index"`
	Peers             []string `json:"peers"`
}

// Server serves the live web dashboard and REST/SSE telemetry endpoints.
type Server struct {
	node     *raft.Node
	sched    *scheduler.Scheduler
	httpAddr string

	server *http.Server

	clientsMu sync.Mutex
	clients   map[chan []byte]bool

	stopCh chan struct{}
}

// NewServer initializes an HTTP dashboard server for the given node and scheduler.
func NewServer(httpAddr string, node *raft.Node, sched *scheduler.Scheduler) *Server {
	return &Server{
		node:     node,
		sched:    sched,
		httpAddr: httpAddr,
		clients:  make(map[chan []byte]bool),
		stopCh:   make(chan struct{}),
	}
}

// Start begins listening on the HTTP address and serving the dashboard.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/submit", s.handleSubmit)

	ln, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		return fmt.Errorf("dashboard listen %s: %w", s.httpAddr, err)
	}

	s.server = &http.Server{
		Addr:         ln.Addr().String(),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // Keep-alive for SSE stream
	}

	go func() {
		_ = s.server.Serve(ln)
	}()

	go s.broadcastLoop()
	return nil
}

// Stop gracefully stops the dashboard HTTP server.
func (s *Server) Stop() {
	close(s.stopCh)
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.server.Shutdown(ctx)
	}
}

func (s *Server) snapshot() ClusterStateView {
	tasks, workers := s.sched.DetailedStatus()
	return ClusterStateView{
		Raft: RaftView{
			NodeID:            s.node.ID,
			Role:              s.node.State().String(),
			Term:              s.node.Term(),
			LeaderID:          s.node.LeaderID(),
			CommitIndex:       s.node.CommitIndex(),
			LastLogIndex:      s.node.LastLogIndex(),
			LastIncludedIndex: s.node.LastIncludedIndex(),
			Peers:             s.node.Peers,
		},
		Workers: workers,
		Tasks:   tasks,
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	f, err := staticFS.Open("static/index.html")
	if err != nil {
		http.Error(w, "Asset not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.Copy(w, f)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	view := s.snapshot()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(view)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Send initial snapshot immediately
	initData, _ := json.Marshal(s.snapshot())
	fmt.Fprintf(w, "data: %s\n\n", initData)
	flusher.Flush()

	clientCh := make(chan []byte, 16)
	s.clientsMu.Lock()
	s.clients[clientCh] = true
	s.clientsMu.Unlock()

	defer func() {
		s.clientsMu.Lock()
		delete(s.clients, clientCh)
		s.clientsMu.Unlock()
	}()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case <-s.stopCh:
			return
		case msg := <-clientCh:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TaskID      string `json:"task_id"`
		Payload     string `json:"payload"`
		Priority    int    `json:"priority"`
		RequiredTag string `json:"required_tag"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.TaskID == "" {
		http.Error(w, "task_id is required", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if s.node.State() != raft.Leader {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": fmt.Sprintf("Node %s is not leader (current leader: %s)", s.node.ID, s.node.LeaderID()),
		})
		return
	}

	_, err := s.sched.SubmitTaskWithOpts(req.TaskID, req.Payload, req.Priority, req.RequiredTag)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true,
	})
	s.broadcast()
}

func (s *Server) broadcast() {
	data, err := json.Marshal(s.snapshot())
	if err != nil {
		return
	}
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	for ch := range s.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

func (s *Server) broadcastLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.broadcast()
		}
	}
}
