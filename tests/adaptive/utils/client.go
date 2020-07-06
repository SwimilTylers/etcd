package utils

import (
	"fmt"
	"go.etcd.io/etcd/tests/adaptive/tests"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

var GlobalClientConfig = make(map[string]interface{})

func InitClientConfig() {
	GlobalClientConfig["shell"] = "#!/usr/bin/env bash"
	GlobalClientConfig["absolute"] = false

	GlobalClientConfig["bench"] = "tools/benchmark/benchmark"
	GlobalClientConfig["bench-srv-urls-generator"] = func(urls []string) string {
		builder := &strings.Builder{}
		builder.WriteString(strings.TrimPrefix(urls[0], "http://"))

		for i := 1; i < len(urls); i++ {
			builder.WriteString(",")
			builder.WriteString(strings.TrimPrefix(urls[i], "http://"))
		}

		return builder.String()
	}
	GlobalClientConfig["bench-arg-format"] = "--endpoints=%edpts --clients=30 --conns=%cns --sample put --key-size=8 --sequential-keys --total=100000 --val-size=256"

	GlobalClientConfig["output-format"] = "run_bench_%size.sh"
}

func UseBenchTool() {
	GlobalClientConfig["bench"] = "tools/benchtool/benchtool"
	GlobalClientConfig["bench-arg-format"] = "--endpoints=%edpts --clients=30 --conns=%cns --database=random[key-size=8,val-size=256] --sample put --total=100000"
}

func CreateBenchVerifyShell(size int) {
	GlobalClientConfig["bench-arg-format"] = strings.ReplaceAll(GlobalClientConfig["bench-arg-format"].(string), "put", "verify")
	GlobalClientConfig["output-format"] = "run_bench_verify_%size.sh"

	CreateBenchShell(size)

	GlobalClientConfig["bench-arg-format"] = strings.ReplaceAll(GlobalClientConfig["bench-arg-format"].(string), "verify", "put")
	GlobalClientConfig["output-format"] = "run_bench_%size.sh"

	CreateBenchShell(size)
}

func CreateBenchShell(size int) {
	s, _ := exec.Command("pwd").Output()
	var benchCmd string
	if GlobalClientConfig["absolute"].(bool) {
		benchCmd = fmt.Sprintf("exec %s/%s", strings.TrimSuffix(string(s), "\n"), GlobalClientConfig["bench"])
	} else {
		benchCmd = fmt.Sprintf("exec %s", GlobalClientConfig["bench"])
	}

	format := GlobalClientConfig["output-format"]
	bench, _ := os.Create(strings.ReplaceAll(format.(string), "%size", strconv.Itoa(size)))
	defer bench.Close()

	srv := GlobalClientConfig["bench-srv-urls-generator"].(func([]string) string)(
		tests.GlobalRunnerConfigs[fmt.Sprintf("c%d", size)].(*tests.CDescriptor).GetClientPorts(),
	)

	args := GlobalClientConfig["bench-arg-format"].(string)
	args = strings.ReplaceAll(args, "%edpts", srv)
	args = strings.ReplaceAll(args, "%cns", strconv.Itoa(size))

	_, _ = bench.WriteString(fmt.Sprintf("%s\n\n%s %s", GlobalClientConfig["shell"], benchCmd, args))
}
