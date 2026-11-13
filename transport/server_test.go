package transport

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

type EchoService struct{}

// Return the supplied value so transport tests can verify a complete RPC round trip.
func (*EchoService) Echo(value int, reply *int) error {
	*reply = value
	return nil
}

// RPC service survives reconnection, while disconnect and shutdown reject further calls.
func TestServerLifecycle(t *testing.T) {
	server := NewServer()
	t.Cleanup(server.Shutdown)
	if err := server.Register("EchoService", &EchoService{}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(listener); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	peer := NewServer()
	t.Cleanup(peer.Shutdown)
	connect := func() {
		t.Helper()
		if err := peer.ConnectToPeer(1, listener.Addr().String()); err != nil {
			t.Fatal(err)
		}
	}
	echo := func() {
		t.Helper()
		var reply int
		if err := peer.Call(1, "EchoService.Echo", 42, &reply); err != nil || reply != 42 {
			t.Fatalf("RPC round trip: reply=%d err=%v", reply, err)
		}
	}
	connect()
	echo()
	connect() // Replacing a connection must preserve RPC service availability.
	echo()
	peer.DisconnectPeer(1)
	var reply int
	if err := peer.Call(1, "EchoService.Echo", 42, &reply); err == nil {
		t.Fatal("call to disconnected peer succeeded")
	}
	connect()
	echo()
	peer.Shutdown()
	peer.Shutdown()
	if err := peer.Call(1, "EchoService.Echo", 42, &reply); err == nil {
		t.Fatal("closed server accepted a call")
	}
	if err := peer.ConnectToPeer(1, listener.Addr().String()); err == nil {
		t.Fatal("closed server accepted a connection")
	}
}

// Closing a connection with no reader must release an outstanding RPC.
func TestShutdownUnblocksStalledRPC(t *testing.T) {
	server := NewServer()
	t.Cleanup(server.Shutdown)
	local, remote := net.Pipe()
	defer remote.Close()
	server.peers[2] = rpc.NewClient(local)
	finished := make(chan error, 1)
	go func() {
		var reply int
		finished <- server.Call(2, "EchoService.Echo", 42, &reply)
	}()
	server.Shutdown()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("stalled call succeeded without a receiver")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not unblock the RPC")
	}
}
