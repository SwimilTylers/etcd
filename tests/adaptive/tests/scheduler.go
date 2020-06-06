package tests

import (
	"go.etcd.io/etcd/embed"
)

type Scheduler struct {
	do  func(int, *embed.Etcd)
	err chan error
	end chan struct{}
}

var DoNothing = Scheduler{do: func(int, *embed.Etcd) {}, err: make(chan error, 100), end: make(chan struct{})}

/*
func GetTimeoutScheduler(d time.Duration) Scheduler {
	return func(idx int, e *embed.Etcd) {
		select {
		case <-e.Server.ReadyNotify():
			log.Printf("Server is ready!")
		case <-time.After(d):
			e.Server.Stop() // trigger a shutdown
			log.Printf("Server took too long to start!")
		}
		log.Fatal(<-e.Err())
	}
}
*/
