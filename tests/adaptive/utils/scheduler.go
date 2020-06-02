package utils

import (
	"go.etcd.io/etcd/embed"
	"log"
	"time"
)

type Scheduler func(int, *embed.Etcd)

var DonNothing Scheduler = func(int, *embed.Etcd) {}

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
