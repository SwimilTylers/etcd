package utils

import (
	"go.etcd.io/etcd/embed"
	"log"
)

func StartNormal(cluster *cDescriptor, idx int) (*embed.Etcd, error) {
	cfg := cluster.GetConfig(idx, embed.ClusterStateFlagNew)
	return embed.StartEtcd(cfg)
}

func RunLocalCluster3(scheduler Scheduler) {
	var srvs [3]*embed.Etcd
	var waits [3]<-chan struct{}

	for i := 0; i < len(srvs); i++ {
		go func(idx int) {
			var err error
			srvs[idx], err = StartNormal(DefaultLocalCluster3, idx)
			if err != nil {
				log.Fatal(err)
			}
			waits[idx] = scheduler(idx, srvs[idx])
		}(i)
	}

	for i := 0; i < 3; i++ {
		select {
		case <-waits[0]:
			log.Println("Srv 0 is closed!")
		case <-waits[1]:
			log.Println("Srv 1 is closed!")
		case <-waits[2]:
			log.Println("Srv 2 is closed!")
		}
	}
}

func Main() {
	RunLocalCluster3(DonNothing)
}
