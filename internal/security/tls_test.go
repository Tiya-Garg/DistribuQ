package security

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/Tiya-Garg/DistribuQ/proto"
	"google.golang.org/grpc"
)

type testSchedulerServer struct {
	pb.UnimplementedSchedulerServiceServer
}

func (t *testSchedulerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Ok: true}, nil
}

func TestMTLSCommunication(t *testing.T) {
	tmpDir := t.TempDir()
	caPEM, certPEM, keyPEM, err := GenerateTestPKI([]string{"localhost", "127.0.0.1"})
	if err != nil {
		t.Fatalf("failed to generate PKI: %v", err)
	}

	caPath := filepath.Join(tmpDir, "ca.crt")
	certPath := filepath.Join(tmpDir, "server.crt")
	keyPath := filepath.Join(tmpDir, "server.key")

	if err := os.WriteFile(caPath, caPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}

	tlsCfg := &TLSConfig{
		CACertPath: caPath,
		CertPath:   certPath,
		KeyPath:    keyPath,
		ServerName: "localhost",
	}

	srvOpt, err := tlsCfg.ServerOption()
	if err != nil {
		t.Fatalf("ServerOption failed: %v", err)
	}

	grpcServer := grpc.NewServer(srvOpt)
	pb.RegisterSchedulerServiceServer(grpcServer, &testSchedulerServer{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// 1. Valid client connection with proper mTLS
	dialOpt, err := tlsCfg.DialOption()
	if err != nil {
		t.Fatalf("DialOption failed: %v", err)
	}

	conn, err := grpc.NewClient(lis.Addr().String(), dialOpt)
	if err != nil {
		t.Fatalf("client dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerServiceClient(conn)
	resp, err := client.Heartbeat(context.Background(), &pb.HeartbeatRequest{WorkerId: "w-mtls"})
	if err != nil || !resp.Ok {
		t.Fatalf("expected successful mTLS RPC call, got resp=%v, err=%v", resp, err)
	}

	// 2. Insecure client should be rejected by mTLS server
	insecureCfg := &TLSConfig{}
	insecureDialOpt, _ := insecureCfg.DialOption()
	badConn, err := grpc.NewClient(lis.Addr().String(), insecureDialOpt)
	if err == nil {
		defer badConn.Close()
		badClient := pb.NewSchedulerServiceClient(badConn)
		_, badErr := badClient.Heartbeat(context.Background(), &pb.HeartbeatRequest{WorkerId: "w-bad"})
		if badErr == nil {
			t.Errorf("expected insecure connection to be rejected by mTLS server, but it succeeded")
		}
	}
}
