package raft

import "testing"

func TestPingPong(t *testing.T) {
	n := 3
	readly := make(chan interface{})
	servers := make([]*Server, n)
	for i := 0; i < n; i++ {
		pids := make([]int, 0)
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			pids = append(pids, j)
		}
		servers[i] = NewServer(i, pids, readly)
		servers[i].Serve()
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				servers[i].ConnectToPeer(j, servers[j].GetAddr())
			}
		}
	}
	close(readly)

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			var reply string
			args := "dummy"
			if err := servers[i].Call(j, "RaftService.Dummy", args, &reply); err != nil {
				t.Errorf("failed on RPC call(%d:%d): %+v", i, j, err)
			}
			sleepMs(300)
		}
	}
}
