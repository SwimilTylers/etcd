package main

import (
	"bufio"
	"flag"
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/tests/adaptive/tests"
	"go.etcd.io/etcd/tests/adaptive/utils"
	"os"
	"time"
)

var (
	restart   = flag.Bool("restart", false, "whether running in restart mode")
	doNothing = flag.Bool("doNothing", false, "whether use doNothing scheduler")
	bench     = flag.String("b", "benchtool", "choose bench tool")
	runner    = flag.String("r", "etcd", "choose runner type")
)

func main() {
	flag.Parse()
	tests.InitRunnerConfig()
	utils.InitClientConfig()

	if *bench == "benchtool" {
		utils.UseBenchTool()
	}

	// change Global Args
	tests.GlobalRunnerConfigs["remain-duration"] = 10 * time.Minute
	benchArgs := utils.ExtractArgs(utils.GlobalClientConfig["bench-arg-format"].(string), "put")
	benchArgs[1]["total"] = "500000"
	benchArgs[0]["clients"] = "24"
	utils.GlobalClientConfig["bench-arg-format"] = utils.MakeArgs(benchArgs, "put")

	// clusters
	tests.GlobalRunnerConfigs["available-machine"] = []string{
		"http://192.168.198.139",
		"http://192.168.198.138",
		"http://192.168.198.137",
	}
	tests.GlobalRunnerConfigs["local-machine"] = "http://192.168.198.137"

	if !*restart {
		if err := RemoveHistory(); err != nil {
			panic("err occurs when remove history")
		}
	}

	var size = 3
	// var hosts []string
	var selected []int
	// tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)] = tests.MakeUniformCluster(size, "http://192.168.198.137")
	// hosts, selected = GetRemoteCluster(size)
	// tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)] = tests.MakeDistinctCluster(hosts)

	switch *bench {
	case "benchmark":
		go utils.CreateBenchShell(size)
	case "benchtool":
		go utils.CreateBenchVerifyShell(size)
	default:
		panic("unknown bench tool")
	}

	var tester tests.TestRunner

	switch *runner {
	case "etcd":
		tester = tests.NormalServerTestRunner
	case "saucr":
		tester = tests.SaucrServerTestRunner
	case "volatile":
		tester = tests.SaucrServerTestRunner
		tests.TurnSaucrIntoVolatile()
	case "persistent":
		tester = tests.SaucrServerTestRunner
		tests.TurnSaucrIntoPersistent()
	default:
		panic("unknown runner type")
	}

	var sch tests.Scheduler

	if *restart || *doNothing {
		sch = tests.DoNothing
	} else {
		sch = MakeModeSwitchScenario(tester.Restart, size, 10*time.Second)
	}

	Pause(GetTesterRunnerInfo(tester, size, selected))

	Run(tester, size, selected...)(sch)
}

func MakeModeSwitchScenario(restart func(*tests.CDescriptor, int) (*embed.Etcd, error), size int, itv time.Duration) tests.Scheduler {
	switch size {
	case 3:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, []int{0}).
			Restart(itv, restart, []int{0}).
			Build()
	case 5:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, []int{0}).
			Shutdown(itv, []int{1}).
			Restart(itv, restart, []int{0}).
			Restart(itv, restart, []int{1}).
			Build()
	case 7:
		return tests.NewSchedulerBuilder(size).
			Init().
			Shutdown(itv, []int{0}).
			Shutdown(itv, []int{1}).
			Shutdown(itv, []int{2}).
			Restart(itv, restart, []int{0}).
			Restart(itv, restart, []int{1}).
			Restart(itv, restart, []int{2}).
			Build()
	default:
		return tests.DoNothing
	}
}

func RemoveHistory() error {
	if err := utils.RemoveAllSrvInfo(); err != nil {
		fmt.Println("cannot remove all srv info: ", err)
		return err
	} else {
		fmt.Println("all past srv info has been removed")
	}

	if err := utils.RemoveAllSrvLog(); err != nil {
		fmt.Println("cannot remove all srv log: ", err)
		return err
	} else {
		fmt.Println("all past srv log has been removed")
	}

	return nil
}

func Run(runner tests.TestRunner, size int, selected ...int) func(scheduler tests.Scheduler) {
	if selected == nil || len(selected) == 0 {
		switch size {
		case 1:
			return runner.Run1
		case 3:
			return runner.Run3
		case 5:
			return runner.Run5
		case 7:
			return runner.Run7
		default:
			return nil
		}
	} else {
		if config, ok := tests.GlobalRunnerConfigs["cx"]; !ok {
			tests.GlobalRunnerConfigs["cx"] = tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)]
		} else if config.(*tests.CDescriptor).GetMemberNum() != size {
			return nil
		}
		return func(s tests.Scheduler) { runner.RunX(selected, s) }
	}
}

func Pause(msg string) {
	_, _ = os.Stdout.WriteString(msg)
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
}

func GetRemoteCluster(size int) ([]string, []int) {
	hosts := make([]string, size)
	var selected []int
	machine := tests.GlobalRunnerConfigs["available-machine"].([]string)
	local := tests.GlobalRunnerConfigs["local-machine"].(string)
	mLen := len(machine)
	for mId := 0; mId < mLen; mId++ {
		for sId := mId; sId < size; sId += mLen {
			hosts[sId] = machine[mId]
			if hosts[sId] == local {
				selected = append(selected, sId)
			}
		}
	}
	return hosts, selected
}

func GetTesterRunnerInfo(tester tests.TestRunner, size int, selected []int) string {
	if selected != nil {
		return fmt.Sprintf("ready for server instantiation [tester=%s,size=%d,selected=%v]", tester.Name, size, selected)
	} else {
		return fmt.Sprintf("ready for server instantiation [tester=%s,size=%d,selected=all]", tester.Name, size)
	}
}
