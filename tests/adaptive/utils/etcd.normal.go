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

	for i := 0; i < len(srvs); i++ {
		var index = i
		var err error
		srvs[i], err = StartNormal(DefaultLocalCluster3, i)
		if err != nil {
			log.Fatal(err)
		}
		go scheduler(index, srvs[i])
	}

	defer func() {
		for _, srv := range srvs {
			srv.Close()
		}
	}()
}

func Main() {
	RunLocalCluster3(DonNothing)
}
