package tests

import (
	"errors"
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"time"
)

type Scheduler struct {
	do  func(int, *embed.Etcd)
	err chan error
	end chan struct{}
}

var DoNothing = Scheduler{do: func(int, *embed.Etcd) {}, err: make(chan error, 100), end: make(chan struct{})}

type SchedulerEvent struct {
	after time.Duration
	event func(etcd *embed.Etcd) (*embed.Etcd, error)
}

type SchedulerBuilder struct {
	events       [][]*SchedulerEvent
	topIsRunning []bool

	srvTerminate   chan struct{}
	totalTerminate chan struct{}
}

func NewSchedulerBuilder(size int) *SchedulerBuilder {
	ret := &SchedulerBuilder{
		events:         make([][]*SchedulerEvent, size),
		topIsRunning:   make([]bool, size),
		srvTerminate:   make(chan struct{}),
		totalTerminate: make(chan struct{}),
	}
	for i := range ret.events {
		ret.events[i] = make([]*SchedulerEvent, 0, 10)
	}
	return ret
}

func (b *SchedulerBuilder) Init() *SchedulerBuilder {
	for i := 0; i < len(b.topIsRunning); i++ {
		b.topIsRunning[i] = true
	}
	return b
}

func (b *SchedulerBuilder) IdleAll(after time.Duration) *SchedulerBuilder {
	idle := make([]bool, len(b.events))
	for i := range idle {
		idle[i] = true
	}

	b.idleInternal(after, idle)
	return b
}

func (b *SchedulerBuilder) Shutdown(after time.Duration, srv []int) *SchedulerBuilder {
	idle := make([]bool, len(b.events))
	for i := range idle {
		idle[i] = true
	}

	for _, s := range srv {
		idle[s] = false
		b.events[s] = append(b.events[s],
			&SchedulerEvent{
				after: after,
				event: func(etcd *embed.Etcd) (*embed.Etcd, error) {
					etcd.Close()
					etcd.Server.Logger().Info("this server is shut down by scheduler",
						zap.String("srv-name", etcd.Server.Cfg.Name),
						zap.String("srv-id", etcd.Server.ID().String()),
					)
					return etcd, nil
				},
			},
		)
		b.topIsRunning[s] = false
	}

	b.idleInternal(after, idle)

	return b
}

func (b *SchedulerBuilder) ShutdownAll(after time.Duration) *SchedulerBuilder {
	for i, e := range b.events {
		b.events[i] = append(e,
			&SchedulerEvent{
				after: after,
				event: func(etcd *embed.Etcd) (*embed.Etcd, error) {
					etcd.Close()
					etcd.Server.Logger().Info("this server is shut down by scheduler",
						zap.String("srv-name", etcd.Server.Cfg.Name),
						zap.String("srv-id", etcd.Server.ID().String()),
					)
					return etcd, nil
				},
			},
		)
		b.topIsRunning[i] = false
	}
	return b
}

func (b *SchedulerBuilder) Restart(after time.Duration, r func(*CDescriptor, int) (*embed.Etcd, error), srv []int) *SchedulerBuilder {
	idle := make([]bool, len(b.events))
	for i := range idle {
		idle[i] = true
	}

	cluster := GlobalRunnerConfigs[fmt.Sprintf("c%d", len(b.events))].(*CDescriptor)
	for _, s := range srv {
		if b.topIsRunning[s] {
			panic("cannot restart a running srv")
		}
		idle[s] = false
		var id = s
		b.events[s] = append(b.events[s],
			&SchedulerEvent{
				after: after,
				event: func(etcd *embed.Etcd) (*embed.Etcd, error) {
					etcd.Server.Logger().Info("this server will restart by scheduler",
						zap.String("srv-name", etcd.Server.Cfg.Name),
						zap.String("srv-id", etcd.Server.ID().String()),
						zap.Int("restart-no", id),
					)
					srv, err := r(cluster, id)
					if err != nil && srv != nil {
						<-srv.Server.ReadyNotify()
					}
					return srv, err
				},
			},
		)
		b.topIsRunning[s] = true
	}

	b.idleInternal(after, idle)

	return b
}

func (b *SchedulerBuilder) daemon() {
	var count = 0
	for s := range b.srvTerminate {
		count++
		if count == len(b.events) {
			b.totalTerminate <- s
			return
		}
	}
}

func (b *SchedulerBuilder) Build() Scheduler {
	sch := Scheduler{
		err: make(chan error, len(b.events)),
		end: b.totalTerminate,
	}

	sch.do = func(i int, etcd *embed.Etcd) {
		event := b.events[i]
		for _, schedulerEvent := range event {
			<-time.After(schedulerEvent.after)
			if schedulerEvent.event != nil {
				var err error
				if etcd, err = schedulerEvent.event(etcd); err != nil {
					sch.err <- errors.New(fmt.Sprintf("from [%d]: %v", i, err))
				}
			}
		}
		<-time.After(GlobalRunnerConfigs["remain-duration"].(time.Duration))
		b.srvTerminate <- struct{}{}
		etcd.Server.Logger().Info("this server is shut down by scheduler",
			zap.String("srv-name", etcd.Server.Cfg.Name),
			zap.String("srv-id", etcd.Server.ID().String()),
		)
	}

	go b.daemon()

	return sch
}

func (b *SchedulerBuilder) idleInternal(after time.Duration, shouldIde []bool) {
	for i, idle := range shouldIde {
		if idle {
			last := len(b.events[i]) - 1
			if len(b.events[i]) == 0 || b.events[i][last].event != nil {
				b.events[i] = append(b.events[i], &SchedulerEvent{
					after: after,
					event: nil,
				})
			} else {
				b.events[i][last].after += after
			}
		}
	}
}

func (b *SchedulerBuilder) syncInternal(after time.Duration, syncC <-chan struct{}, count int, shouldSync []bool) {

}

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
