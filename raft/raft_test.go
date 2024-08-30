package raft

import (
	"testing"
)

// リーダーの整合性を確認する
func TestElection(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

// リーダーが disconnected となった時の挙動を確認する
// 選挙が行われ、新たなリーダーが選出される
func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	originLeader, originTerm := h.CheckSingleLeader()

	h.DisconnectPeer(originLeader)
	sleepMs(300)

	newLeader, newTerm := h.CheckSingleLeader()
	if newLeader == originLeader {
		t.Errorf("expect new leader to be different from origin leader, but got the same id: %d", newLeader)
	}
	if newTerm <= originTerm {
		t.Errorf("expect newTerm > originTerm, but got newTerm=%d, originTerm=%d", newTerm, originTerm)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	h.DisconnectPeer(origLeaderId)
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)

	sleepMs(450)
	h.CheckNoLeader()

	h.ReconnectPeer(otherId)
	h.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	// disconnect all
	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}
	sleepMs(300)
	h.CheckNoLeader()

	// reconnect all
	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	sleepMs(300)
	h.CheckSingleLeader()
}

func TestDisconnectThenReconnectOne(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	originLeader, _ := h.CheckSingleLeader()
	h.DisconnectPeer(originLeader)
	sleepMs(300)

	newLeader, _ := h.CheckSingleLeader()

	h.ReconnectPeer(originLeader)
	sleepMs(300)

	againLeader, _ := h.CheckSingleLeader()

	if newLeader != againLeader {
		t.Errorf("got %d, want %d", againLeader, newLeader)
	}
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(350)

	newLeaderId, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderId)
	sleepMs(150)

	againLeaderId, againTerm := h.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}
