package tests

import (
	"go.etcd.io/etcd/embed"
	"log"
)

func startOneNormal(cluster *cDescriptor, idx int) (*embed.Etcd, error) {
	cfg := cluster.GetConfig(idx, embed.ClusterStateFlagNew)
	return embed.StartEtcd(cfg)
}

var NormalServerTestRunner = TestRunner{
	Run1: func(scheduler Scheduler) {
		srv, err := startOneNormal(GlobalRunnerConfigs["c1"].(*cDescriptor), 0)
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
		s := make([]*embed.Etcd, 3)
		for i := 0; i < 3; i++ {
			go func(idx int) {
				srv, err := startOneNormal(GlobalRunnerConfigs["c3"].(*cDescriptor), idx)
				s[idx] = srv
				if err != nil {
					// if the server cannot init properly, stop the test immediately
					scheduler.err <- err
					scheduler.end <- struct{}{}
					return
				}
				<-srv.Server.ReadyNotify()
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
				log.Println("NormalServerTestRunner.Run3 terminated!")
				return
			}
		}
	},
	Run5: nil,
	Run7: nil,
}
