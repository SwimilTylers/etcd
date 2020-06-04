package utils

import (
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver"
	"log"
)

func startOneSaucr(cluster *cDescriptor, idx int, sCfg *etcdserver.SaucrConfig) (*embed.Etcd, error) {
	cfg := cluster.GetConfig(idx, embed.ClusterStateFlagNew)
	return embed.StartSaucrEtcd(cfg, sCfg)
}

var EtcdServerTestRunner = TestRunner{
	Run1: nil,
	Run3: func(scheduler Scheduler) {
		s := make([]*embed.Etcd, 3)
		for i := 0; i < 3; i++ {
			go func(idx int) {
				srv, err := startOneSaucr(DefaultLocalCluster3, idx, nil)
				s[idx] = srv
				if err != nil {
					// if the server cannot init properly, stop the test immediately
					scheduler.err <- err
					scheduler.end <- struct{}{}
					return
				}
				scheduler.do(idx, srv)
			}(i)
		}

		defer func() {
			for _, etcd := range s {
				etcd.Close()
			}
		}()

		for {
			select {
			case e := <-scheduler.err:
				log.Fatal(e)
			case <-scheduler.end:
				log.Println("EtcdServerTestRunner.Run3 terminated!")
				return
			}
		}
	},
	Run5: nil,
	Run7: nil,
}
