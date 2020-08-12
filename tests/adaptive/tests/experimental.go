package tests

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func UseExperimentalTesterRunner(runner TestRunner, desc string) TestRunner {
	switch desc {
	case "goroutine-based":
		GlobalRunnerConfigs["g_runner"] = runner

		name := strings.Join([]string{runner.Name, "[Goroutine version]"}, "")
		start := runner.Start
		restart := runner.Restart

		runner = GeneralGoroutineTestRunner

		runner.Name = name
		runner.Start = start
		runner.Restart = restart

		return runner
	case "process-based":
		GlobalRunnerConfigs["g_runner"] = runner

		name := strings.Join([]string{runner.Name, "[Process version]"}, "")
		start := runner.Start
		restart := runner.Restart

		runner = GeneralProcessTestRunner

		runner.Name = name
		runner.Start = start
		runner.Restart = restart

		return runner
	default:
		panic("unknown experimental test runner")
	}
}

func ForkExecExperiment(arg0 string, argv []string) *exec.Cmd {
	cmd := exec.Command(arg0, argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		fmt.Println("receive an error:", err)
	}
	return cmd
}

func MasterExperiment(cmd *exec.Cmd) {
	fmt.Println("this pid:", os.Getpid(), "sub pid:", cmd.Process.Pid)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch)

	for {
		select {
		case o := <-ch:
			fmt.Println("[", os.Getpid(), "] <-", o)
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			cmd.Process.Wait()
			fmt.Println("[", os.Getpid(), "] kills", cmd.Process.Pid)
			time.Sleep(10 * time.Second)
			return
		}
	}
}

func SlaveExperiment() {
	fmt.Println("this pid:", os.Getpid())

	ch := make(chan os.Signal, 1)
	signal.Notify(ch)

	for o := range ch {
		fmt.Println("[", os.Getpid(), "] <-", o)
	}
}
