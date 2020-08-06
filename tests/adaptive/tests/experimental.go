package tests

import "strings"

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
	case "goroutine-panic-based":
		GlobalRunnerConfigs["g_runner"] = runner

		name := strings.Join([]string{runner.Name, "[Goroutine version][using panic/recover mechanism]"}, "")
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
