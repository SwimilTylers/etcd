package main

import (
	"bufio"
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/tests/adaptive/tests"
	"go.etcd.io/etcd/tests/adaptive/utils"
	"os"
	"time"
)

func main() {
	tests.InitRunnerConfig()
	utils.InitClientConfig()

	utils.UseBenchTool()

	// change Global Args
	tests.GlobalRunnerConfigs["remain-duration"] = 5 * time.Minute
	benchArgs := utils.ExtractArgs(utils.GlobalClientConfig["bench-arg-format"].(string), "put")
	benchArgs[1]["total"] = "400000"
	utils.GlobalClientConfig["bench-arg-format"] = utils.MakeArgs(benchArgs, "put")

	if err := utils.RemoveAllSrvInfo(); err != nil {
		fmt.Println("cannot remove all srv info: ", err)
		return
	} else {
		fmt.Println("all past srv info has been removed")
	}

	if err := utils.RemoveAllSrvLog(); err != nil {
		fmt.Println("cannot remove all srv log: ", err)
		return
	} else {
		fmt.Println("all past srv log has been removed")
	}

	var size = 5
	// tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)] = tests.MakeUniformCluster(size, "http://192.168.198.137")

	go utils.CreateBenchShell(size)

	tester := tests.NormalServerTestRunner
	// tester := tests.SaucrServerTestRunner

	// sch := MakeModeSwitchScenario(tester.Restart, size, 10*time.Second)
	// sch := tests.DoNothing

	// tests.TurnSaucrIntoVolatile()
	// tests.TurnSaucrIntoPersistent()
	// Run(tester, size)(sch)

	// tests.NormalServerTestRunner.Run7(tests.DoNothing)
	// tests.SaucrServerTestRunner.Run5(tests.DoNothing)
	Pause("ready for validation")

	Run(tester, size)(tests.DoNothing)
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

func Run(runner tests.TestRunner, size int) func(scheduler tests.Scheduler) {
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
}

func Pause(msg string) {
	_, _ = os.Stdout.WriteString(msg)
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
}
