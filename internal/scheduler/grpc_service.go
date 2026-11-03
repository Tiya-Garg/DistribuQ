package scheduler

import (
	"context"
	"errors"
	"log"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCSchedulerServer implements pb.SchedulerServiceServer to serve scheduler RPCs over gRPC.
type GRPCSchedulerServer struct {
	pb.UnimplementedSchedulerServiceServer
	S        *Scheduler
	IsLeader func() bool
}

func (s *GRPCSchedulerServer) requireLeader() bool {
	return s.IsLeader == nil || s.IsLeader()
}

func (s *GRPCSchedulerServer) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	s.S.RegisterWorker(req.WorkerId, req.Addr)
	return &pb.RegisterWorkerResponse{Ok: true}, nil
}

func (s *GRPCSchedulerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	err := s.S.Heartbeat(req.WorkerId)
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
	if _, err := s.S.SubmitTaskWithPriority(req.TaskId, req.Payload, int(req.Priority)); err != nil {
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
