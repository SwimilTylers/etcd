package tests

import (
	"errors"
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type MasterProcessKit struct {
	logger *zap.Logger
	argv   [][]string
	rArgv  [][]string

	sProcess []*os.Process
	sId      []types.ID

	usingEtcdCtl bool
	ctlPath      string

	shutGen    ShutGen
	restartGen RestartGen

	sWg *sync.WaitGroup

	lPort int
}

type MasterProcessRPC struct {
	relay chan<- interface{}
}

type MasterProcessRPCArg struct {
	Header string
	Idx    int
	Id     types.ID
}

func (mpr *MasterProcessRPC) ClusterEstablished(arg MasterProcessRPCArg, accept *bool) error {
	select {
	case mpr.relay <- arg:
		*accept = true
	case <-time.After(50 * time.Millisecond):
		*accept = false
	}
	return nil
}

func (mpk *MasterProcessKit) Daemon() {
	defer mpk.logger.Sync()

	relay := make(chan interface{}, 2*len(mpk.sProcess))

	r := &MasterProcessRPC{relay: relay}

	rpc.Register(r)
	rpc.HandleHTTP()

	if l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", mpk.lPort)); err == nil {
		go http.Serve(l, nil)
	} else {
		panic("master scheduler rpc broken")
	}

	for m := range relay {
		arg := m.(MasterProcessRPCArg)

		if arg.Header == "START" {
			mpk.sWg.Done()
		}

		mpk.sId[arg.Idx] = arg.Id
		mpk.logger.Info("receive rpc call from slave", zap.Int("slave-id", arg.Idx), zap.String("slave-server-id", arg.Id.String()))
	}
}

// StartX starts multiple slave processes at one time
// This function should call at most one time in a master lifetime.
func (mpk *MasterProcessKit) StartX(srv ...int) <-chan struct{} {
	mpk.sWg.Add(len(srv))
	for _, id := range srv {
		cmd := exec.Command(mpk.argv[id][0], mpk.argv[id][1:]...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			mpk.logger.Error("fail to start a slave process",
				zap.Error(err),
				zap.Int("slave-id", id),
				zap.Strings("cmd", mpk.argv[id]),
			)
			continue
		}

		mpk.logger.Info("slave process started",
			zap.Int("slave-id", id),
			zap.Int("slave-pid", cmd.Process.Pid),
			zap.Strings("cmd", mpk.argv[id]),
		)

		mpk.sProcess[id] = cmd.Process
	}

	result := make(chan struct{})
	go func() {
		mpk.sWg.Wait()
		result <- struct{}{}
	}()

	return result
}

func (mpk *MasterProcessKit) RestartX(srv ...int) {
	for _, id := range srv {
		cmd := exec.Command(mpk.rArgv[id][0], mpk.rArgv[id][1:]...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			mpk.logger.Error("fail to restart a slave process",
				zap.Error(err),
				zap.Int("slave-id", id),
				zap.Strings("cmd", mpk.rArgv[id]),
			)
			continue
		}

		mpk.logger.Info("slave process restarted",
			zap.Int("slave-id", id),
			zap.Int("slave-pid", cmd.Process.Pid),
			zap.Strings("cmd", mpk.rArgv[id]),
		)

		mpk.sProcess[id] = cmd.Process
	}
}

func (mpk *MasterProcessKit) ShutX(srv ...int) {
	for _, id := range srv {
		p := mpk.sProcess[id]
		if p == nil {
			mpk.logger.Error("fail to find slave process",
				zap.Int("slave-id", id),
			)
			continue
		}

		if err := p.Kill(); err != nil {
			mpk.logger.Error("fail to kill slave process",
				zap.Error(err),
				zap.Int("slave-id", id),
				zap.Int("slave-pid", p.Pid),
			)
			continue
		}
		if _, err := p.Wait(); err != nil {
			mpk.logger.Error("fail to wait for slave process",
				zap.Error(err),
				zap.Int("slave-id", id),
				zap.Int("slave-pid", p.Pid),
			)
			continue
		}

		mpk.logger.Info("slave process killed",
			zap.Int("slave-id", id),
			zap.Int("slave-pid", p.Pid),
		)
	}
}

func NewMasterProcessKit(size int, seed int64, lPort int) *MasterProcessKit {
	lg, _ := GetSchedulerLogger()

	kit := &MasterProcessKit{
		logger:   lg,
		sWg:      &sync.WaitGroup{},
		sProcess: make([]*os.Process, size),
		sId:      make([]types.ID, size),
		argv:     make([][]string, size),
		rArgv:    make([][]string, size),
		lPort:    lPort,
	}

	if p, ok := GlobalRunnerConfigs["etcdctl"]; ok {
		kit.usingEtcdCtl = true
		kit.ctlPath = p.(string)
	} else {
		kit.usingEtcdCtl = false
	}

	for i := 0; i < size; i++ {
		kit.argv[i] = WriteArgvForSlave(i, seed, false)
		kit.rArgv[i] = WriteArgvForSlave(i, seed, true)
	}

	kit.shutGen = func(cluster *CDescriptor, id int) func(etcd *embed.Etcd) (*embed.Etcd, error) {
		return func(etcd *embed.Etcd) (*embed.Etcd, error) {
			p := kit.sProcess[id]
			if p == nil {
				kit.logger.Error("fail to find slave process",
					zap.Int("slave-id", id),
				)
				return etcd, errors.New("pid missing")
			}

			if err := p.Kill(); err != nil {
				kit.logger.Error("fail to kill slave process",
					zap.Error(err),
					zap.Int("slave-id", id),
					zap.Int("slave-pid", p.Pid),
				)
				return etcd, err
			}
			if _, err := p.Wait(); err != nil {
				kit.logger.Error("fail to wait for slave process",
					zap.Error(err),
					zap.Int("slave-id", id),
					zap.Int("slave-pid", p.Pid),
				)
				return etcd, err
			}

			kit.logger.Info("slave process killed",
				zap.Int("slave-id", id),
				zap.Int("slave-pid", p.Pid),
			)

			if kit.usingEtcdCtl {
				if s, err := RemoveMemberUsingEtcdctl(kit.ctlPath, kit.sId[id], cluster.members); err != nil {
					kit.logger.Error("slave server removed from cluster failed",
						zap.Error(err),
						zap.String("etcd-ctl-path", kit.ctlPath),
						zap.Int("slave-id", id),
						zap.String("slave-server-id", kit.sId[id].String()),
					)
				} else {
					kit.logger.Info("slave server removed from cluster",
						zap.String("etcd-ctl-path", kit.ctlPath),
						zap.Int("slave-id", id),
						zap.String("slave-server-id", kit.sId[id].String()),
						zap.String("exec-output", string(s)),
					)
				}
			} else {
				kit.logger.Info("skip cluster re-conf",
					zap.String("op", "shut"),
					zap.Int("slave-id", id),
					zap.String("slave-server-id", kit.sId[id].String()),
				)
			}

			return etcd, nil
		}
	}
	kit.restartGen = func(cluster *CDescriptor, id int, r func(*CDescriptor, int) (*embed.Etcd, error)) func(etcd *embed.Etcd) (*embed.Etcd, error) {
		return func(etcd *embed.Etcd) (*embed.Etcd, error) {
			if kit.usingEtcdCtl {
				member := cluster.members
				if s, err := AddMemberUsingEtcdctl(kit.ctlPath, member[id], member); err != nil {
					kit.logger.Error("slave server pre-added to cluster failed",
						zap.Error(err),
						zap.String("etcd-ctl-path", kit.ctlPath),
						zap.Int("slave-id", id),
					)
				} else {
					kit.logger.Info("slave server pre-added to cluster",
						zap.String("etcd-ctl-path", kit.ctlPath),
						zap.Int("slave-id", id),
						zap.String("exec-output", string(s)),
					)
				}
			} else {
				kit.logger.Info("skip cluster re-conf",
					zap.String("op", "restart"),
					zap.Int("slave-id", id),
				)
			}

			cmd := exec.Command(kit.rArgv[id][0], kit.rArgv[id][1:]...)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			if err := cmd.Start(); err != nil {
				kit.logger.Error("fail to start a slave process",
					zap.Error(err),
					zap.Int("slave-id", id),
					zap.Strings("cmd", kit.rArgv[id]),
				)
				return etcd, err
			}

			kit.logger.Info("slave process started",
				zap.Int("slave-id", id),
				zap.Int("slave-pid", cmd.Process.Pid),
				zap.Strings("cmd", kit.rArgv[id]),
			)

			kit.sProcess[id] = cmd.Process

			return etcd, nil
		}
	}
	return kit
}

func WriteArgvForSlave(id int, seed int64, restart bool) []string {
	return GlobalRunnerConfigs["root-argv-builder"].(*ArgvBuilder).
		CopyOf().                                      // 1. Make a copy of argv dict
		AddFlag("-selected", fmt.Sprintf("[%d]", id)). // 2. Designate server id
		AddFlag("-role", "slave").                     // 3. Run in slave mode
		AddFlag("-seed", strconv.FormatInt(seed, 10)). // 4. Designate a common random seed
		AddFlag("-s", "do nothing").                   // 5. Disable slave's scheduler
		AddBoolFlag("-restart", restart).              // 6. Whether running in a restart mode
		AddBoolFlag("-V", false).                      // 7. Mute the slave
		RemoveFlag("-using-etcdctl-path").             // 8. Turn off etcdctl option
		Align().                                       // 9. Generate a new argv dict
		StringSlice()
}

func WorkInMasterRole(size int, seed int64, schPort int) {
	// unmark flag
	if _, ok := GlobalRunnerConfigs["slave_process"]; ok {
		delete(GlobalRunnerConfigs, "slave_process")
	}

	GlobalRunnerConfigs["scheduler-log-file"] = "test-master-sch.log"

	InitRootArgvBuilder()

	kit := NewMasterProcessKit(size, seed, schPort)

	GlobalRunnerConfigs["master-kit"] = kit
	GlobalRunnerConfigs["sch-shut"] = kit.shutGen
	GlobalRunnerConfigs["sch-restart"] = kit.restartGen

	go kit.Daemon()
}

func WorkInSlaveRole(slaveId int, schPort int, restart bool) {
	GlobalRunnerConfigs["scheduler-log-file"] = fmt.Sprintf("test-slave-%d-sch.log", slaveId)
	if restart {
		GlobalRunnerConfigs["rpc-header"] = "RESTART"
	} else {
		GlobalRunnerConfigs["rpc-header"] = "START"
	}

	if client, err := rpc.DialHTTP("tcp", fmt.Sprintf("127.0.0.1:%d", schPort)); err == nil {
		// mark flag
		GlobalRunnerConfigs["slave_process"] = func(idx int, id types.ID) {
			var reply bool
			s := GlobalRunnerConfigs["rpc-header"].(string)
			if err := client.Call("MasterProcessRPC.ClusterEstablished", MasterProcessRPCArg{Header: s, Id: id, Idx: idx}, &reply); err != nil {
				panic(fmt.Sprintf("failed to call master's rpc, err=%v", err))
			}
		}
	} else {
		// mark flag
		GlobalRunnerConfigs["slave_process"] = err
		panic(strings.Join([]string{"slave process rpc broken", err.Error()}, " "))
	}

}
