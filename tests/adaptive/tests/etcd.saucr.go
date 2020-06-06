package tests

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
	Run1: func(scheduler Scheduler) {
		cluster, sCfg := GlobalRunnerConfigs["c1"].(*cDescriptor), GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

		srv, err := startOneSaucr(cluster, 0, sCfg)
		if err != nil {
			log.Fatal(err)
			return
		}
		go func() {
			<-srv.Server.ReadyNotify()
			scheduler.do(0, srv)
		}()

		defer srv.Close()

		for {
			select {
			case e := <-scheduler.err:
				log.Fatal(e)
			case <-scheduler.end:
				log.Println("NormalServerTestRunner.Run1 terminated!")
				return
			}
		}
	},
	Run3: func(scheduler Scheduler) {
		cluster, sCfg := GlobalRunnerConfigs["c3"].(*cDescriptor), GlobalRunnerConfigs["saucr"].(*etcdserver.SaucrConfig)

		s := make([]*embed.Etcd, 3)
		for i := 0; i < 3; i++ {
			go func(idx int) {
				srv, err := startOneSaucr(cluster, idx, sCfg)
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
