package etcdserver

import (
	"errors"
	"fmt"
	"go.uber.org/zap"
	"time"
)

// this file lists out several functions used in crash failure simulation

var (
	substituteStopFunc = map[string]func(o ...interface{}){

		"EtcdServer.Stop: without leader transfer": func(o ...interface{}) {
			cascadedStopFunc["EtcdServer.HardStop"](o)
		},
	}

	cascadedStopFunc = make(map[string]func(o ...interface{}))
)

func InitCrashSimulation() {
	cascadedStopFunc["EtcdServer.Stop"] = func(o ...interface{}) {
		switch o[0].(type) {
		case *EtcdServer:
			s := o[0].(*EtcdServer)
			if err := s.TransferLeadership(); err != nil {
				if lg := s.getLogger(); lg != nil {
					lg.Warn("leadership transfer failed", zap.String("local-member-id", s.ID().String()), zap.Error(err))
				} else {
					plog.Warningf("%s failed to transfer leadership (%v)", s.ID(), err)
				}
			}
		case *EtcdSaucrServer:
			s := o[0].(*EtcdSaucrServer)
			if err := s.TransferLeadership(); err != nil {
				if lg := s.getLogger(); lg != nil {
					lg.Warn("leadership transfer failed", zap.String("local-member-id", s.ID().String()), zap.Error(err))
				} else {
					plog.Warningf("%s failed to transfer leadership (%v)", s.ID(), err)
				}
			}
		}

		cascadedStopFunc["EtcdServer.HardStop"](o)
	}
}

var (
	etcdCrashComponent = map[string]func(server *EtcdServer){
		"node": func(server *EtcdServer) {
			server.r.Stop()
		},
		"raft": func(server *EtcdServer) {
			server.r.Node.Stop()
		},
		"net": func(server *EtcdServer) {
			server.r.transport.Stop()
		},
		"disk": func(server *EtcdServer) {
			server.r.storage.Close()
		},
	}
	saucrCrashComponent = map[string]func(server *EtcdSaucrServer){
		"node": func(server *EtcdSaucrServer) {
			server.srn.Stop()
		},
		"raft": func(server *EtcdSaucrServer) {
			server.srn.Node.Stop()
		},
		"net": func(server *EtcdSaucrServer) {
			server.srn.transport.Stop()
		},
		"disk": func(server *EtcdSaucrServer) {
			server.srn.PManager.Close()
		},
	}
)

func CrashComponent(lg *zap.Logger, component string, s0 *EtcdServer, c0 func(server *EtcdServer), s1 *EtcdSaucrServer, c1 func(server *EtcdSaucrServer), wait time.Duration) {
	defer func() {
		if err := recover(); err != nil {
			lg.Error("catch an error when crashing a component", zap.String("component", component), zap.Error(errors.New(fmt.Sprintf("%v", err))))
		}
	}()

	ch := make(chan struct{})

	if s0 != nil {
		if c0 == nil {
			lg.Error("component crash function not found", zap.String("component", component))
			return
		}

		go func() {
			c0(s0)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
			lg.Info("component crashed", zap.String("component", component))
		case <-time.After(wait):
			lg.Error("component crash time out", zap.String("component", component))
		}

		return
	}

	if s1 != nil {
		if c1 == nil {
			lg.Error("component crash function not found", zap.String("component", component))
			return
		}

		go func() {
			c1(s1)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
			lg.Info("component crashed", zap.String("component", component))
		case <-time.After(wait):
			lg.Error("component crash time out", zap.String("component", component))
		}

		return
	}
}

func CrashAccordingly(etcd *EtcdServer, saucr *EtcdSaucrServer, seq []string, wait time.Duration) {
	if etcd != nil {
		for _, component := range seq {
			if f, ok := etcdCrashComponent[component]; ok {
				CrashComponent(etcd.lg, component, etcd, f, nil, nil, wait)
			} else {
				CrashComponent(etcd.lg, component, etcd, nil, nil, nil, wait)
			}
		}
		return
	}

	if saucr != nil {
		for _, component := range seq {
			if f, ok := saucrCrashComponent[component]; ok {
				CrashComponent(saucr.lg, component, nil, nil, saucr, f, wait)
			} else {
				CrashComponent(saucr.lg, component, nil, nil, saucr, nil, wait)
			}
		}
		return
	}
}
