package raftkv

import (
	"net"
	"testing"
)

// Serving starts consensus once; shutdown is repeatable and prevents restart.
func TestNodeLifecycle(t *testing.T) {
	node := NewNode(1, []int{2})
	t.Cleanup(node.Shutdown)
	if status := node.Status(); status.ID != 1 || status.Started || status.Stopped {
		t.Fatalf("unexpected initial status: %+v", status)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Serve(listener); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if status := node.Status(); !status.Started || status.Stopped {
		t.Fatalf("Serve did not start consensus: %+v", status)
	}
	if err := node.Serve(listener); err == nil {
		t.Fatal("node served twice")
	}
	node.Shutdown()
	node.Shutdown()
	if status := node.Status(); !status.Stopped {
		t.Fatalf("Shutdown did not stop consensus: %+v", status)
	}
	if err := node.Serve(listener); err == nil {
		t.Fatal("stopped node restarted")
	}
}

// An unused node can be stopped without ever starting consensus.
func TestNodeShutdownBeforeServe(t *testing.T) {
	node := NewNode(1, nil)
	node.Shutdown()
	if status := node.Status(); status.Started || !status.Stopped {
		t.Fatalf("unexpected stopped status: %+v", status)
	}
}
