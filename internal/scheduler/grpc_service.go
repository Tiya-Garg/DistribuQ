package scheduler

import (
	"context"
	"errors"
	"log"
	"time"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxLoggedCause caps how many runes of a failure cause we print in logs.
const maxLoggedCause = 500

func truncateCause(cause string) string {
	r := []rune(cause)
	if len(r) <= maxLoggedCause {
		return cause
	}
	return string(r[:maxLoggedCause]) + "..."
}

// GRPCSchedulerServer implements pb.SchedulerServiceServer to serve scheduler RPCs over gRPC.
type GRPCSchedulerServer struct {
	pb.UnimplementedSchedulerServiceServer
	S            *Scheduler
	IsLeader     func() bool
	AddPeerFn    func(peerID, peerAddr string) error
	RemovePeerFn func(peerID string) error
	GetPeersFn   func() (leaderID string, peers []string)
}

func (s *GRPCSchedulerServer) requireLeader() bool {
	return s.IsLeader == nil || s.IsLeader()
}

func (s *GRPCSchedulerServer) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	s.S.RegisterWorkerWithOpts(req.WorkerId, req.Addr, req.Tags, int(req.MaxConcurrent))
	return &pb.RegisterWorkerResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) DrainWorker(ctx context.Context, req *pb.DrainWorkerRequest) (*pb.DrainWorkerResponse, error) {
	err := s.S.DrainWorker(req.WorkerId)
	if err != nil {
		if errors.Is(err, ErrUnknownWorker) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.DrainWorkerResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	err := s.S.HeartbeatWithTelemetry(req.WorkerId, int(req.ActiveTasks), req.CpuUsage)
	if err != nil {
		if errors.Is(err, ErrUnknownWorker) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.HeartbeatResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	if !s.requireLeader() {
		return nil, status.Error(codes.FailedPrecondition, ErrNotLeader.Error())
	}
	var runAt time.Time
	if req.RunAtUnixMs > 0 {
		runAt = time.UnixMilli(req.RunAtUnixMs)
	}
	if _, err := s.S.SubmitTaskFull(req.TaskId, req.Payload, int(req.Priority), req.RequiredTag, runAt); err != nil {
		if errors.Is(err, ErrDuplicateTask) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		if errors.Is(err, ErrEmptyTaskID) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SubmitTaskResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) CompleteTask(ctx context.Context, req *pb.CompleteTaskRequest) (*pb.CompleteTaskResponse, error) {
	err := s.S.CompleteTaskBy(req.TaskId, req.WorkerId)
	if err != nil {
		if errors.Is(err, ErrStaleReport) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CompleteTaskResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) FailTask(ctx context.Context, req *pb.FailTaskRequest) (*pb.FailTaskResponse, error) {
	err := s.S.FailTaskBy(req.TaskId, req.WorkerId)
	cause := truncateCause(req.Cause)
	switch {
	case err == nil:
		log.Printf("scheduler: task %s failed on worker %s: %s", req.TaskId, req.WorkerId, cause)
	case errors.Is(err, ErrStaleReport):
		log.Printf("scheduler: ignoring stale failure report for task %s from worker %s: %s", req.TaskId, req.WorkerId, cause)
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.FailTaskResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	tasks, workerCount := s.S.Snapshot()
	tasksMap := make(map[string]string, len(tasks))
	for k, v := range tasks {
		tasksMap[k] = string(v)
	}
	return &pb.GetStatusResponse{
		Tasks:       tasksMap,
		WorkerCount: int32(workerCount),
	}, nil
}

func (s *GRPCSchedulerServer) GetDLQ(ctx context.Context, req *pb.GetDLQRequest) (*pb.GetDLQResponse, error) {
	dlq := s.S.GetDLQTasks()
	tasks := make([]*pb.TaskInfo, 0, len(dlq))
	for _, t := range dlq {
		tasks = append(tasks, &pb.TaskInfo{
			Id:          t.ID,
			Payload:     t.Payload,
			Status:      string(t.Status),
			WorkerId:    t.WorkerID,
			Attempts:    int32(t.Attempts),
			MaxRetries:  int32(t.MaxRetries),
			Priority:    int32(t.Priority),
			RequiredTag: t.RequiredTag,
			LastError:   t.LastError,
		})
	}
	return &pb.GetDLQResponse{Tasks: tasks}, nil
}

func (s *GRPCSchedulerServer) ReplayDLQ(ctx context.Context, req *pb.ReplayDLQRequest) (*pb.ReplayDLQResponse, error) {
	if !s.requireLeader() {
		return nil, status.Error(codes.FailedPrecondition, ErrNotLeader.Error())
	}
	if err := s.S.ReplayDLQTask(req.TaskId); err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ReplayDLQResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) PurgeDLQ(ctx context.Context, req *pb.PurgeDLQRequest) (*pb.PurgeDLQResponse, error) {
	if !s.requireLeader() {
		return nil, status.Error(codes.FailedPrecondition, ErrNotLeader.Error())
	}
	if err := s.S.PurgeDLQTask(req.TaskId); err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.PurgeDLQResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	if !s.requireLeader() {
		return nil, status.Error(codes.FailedPrecondition, ErrNotLeader.Error())
	}
	if s.AddPeerFn == nil {
		return nil, status.Error(codes.Unimplemented, "membership changes not supported on this node")
	}
	if err := s.AddPeerFn(req.PeerId, req.PeerAddr); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.AddPeerResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error) {
	if !s.requireLeader() {
		return nil, status.Error(codes.FailedPrecondition, ErrNotLeader.Error())
	}
	if s.RemovePeerFn == nil {
		return nil, status.Error(codes.Unimplemented, "membership changes not supported on this node")
	}
	if err := s.RemovePeerFn(req.PeerId); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.RemovePeerResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) GetClusterMembership(ctx context.Context, req *pb.ClusterMembershipRequest) (*pb.ClusterMembershipResponse, error) {
	if s.GetPeersFn == nil {
		return &pb.ClusterMembershipResponse{}, nil
	}
	leader, peers := s.GetPeersFn()
	return &pb.ClusterMembershipResponse{
		LeaderId: leader,
		Peers:    peers,
	}, nil
}
