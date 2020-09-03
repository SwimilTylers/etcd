package tests

import (
	"fmt"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
	"net/url"
	"os/exec"
	"strings"
)

type SDescriptor struct {
	name  string
	cPort *url.URL
	pPort *url.URL
}

type CDescriptor struct {
	token   string
	members []*SDescriptor

	lg     string
	output string
}

func (c *CDescriptor) Cluster() string {
	var b strings.Builder
	b.WriteString(c.members[0].name + "=" + c.members[0].pPort.String())
	for i := 1; i < len(c.members); i++ {
		b.WriteString("," + c.members[i].name + "=" + c.members[i].pPort.String())
	}
	return b.String()
}

func (c *CDescriptor) GetConfig(idx int, clusterState string) *embed.Config {
	cfg := embed.NewConfig()

	var srv = c.members[idx]

	if srv != nil {
		cfg.Name = srv.name
		cfg.Dir = c.token + "." + srv.name

		cfg.ACUrls = []url.URL{*srv.cPort}
		cfg.LCUrls = []url.URL{*srv.cPort}

		cfg.APUrls = []url.URL{*srv.pPort}
		cfg.LPUrls = []url.URL{*srv.pPort}

		cfg.ClusterState = clusterState
		cfg.InitialClusterToken = c.token
		cfg.InitialCluster = c.Cluster()

		cfg.Logger = c.lg
		if f, ok := GlobalRunnerConfigs["log-file-format"]; ok {
			cfg.LogOutputs = []string{c.output, fmt.Sprintf(f.(string), idx, len(c.members))}
		} else {
			cfg.LogOutputs = []string{c.output}
		}
	}

	return cfg
}

func (c *CDescriptor) GetClientPorts() []string {
	cps := make([]string, len(c.members))
	for i, member := range c.members {
		cps[i] = member.cPort.String()
	}
	return cps
}

func (c *CDescriptor) GetMemberNum() int {
	return len(c.members)
}

func GetUrl(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

var DefaultLocalCluster1 = &CDescriptor{
	token: "test-local-1",
	members: []*SDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}

var DefaultLocalCluster3 = &CDescriptor{
	token: "test-local-3",
	members: []*SDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
		{"srv1", GetUrl("http://127.0.0.1:22379"), GetUrl("http://127.0.0.1:22380")},
		{"srv2", GetUrl("http://127.0.0.1:32379"), GetUrl("http://127.0.0.1:32380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}

var DefaultLocalCluster5 = &CDescriptor{
	token: "test-local-5",
	members: []*SDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:11379"), GetUrl("http://127.0.0.1:11380")},
		{"srv1", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
		{"srv2", GetUrl("http://127.0.0.1:13379"), GetUrl("http://127.0.0.1:13380")},
		{"srv3", GetUrl("http://127.0.0.1:14379"), GetUrl("http://127.0.0.1:14380")},
		{"srv4", GetUrl("http://127.0.0.1:15379"), GetUrl("http://127.0.0.1:15380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}

var DefaultLocalCluster7 = &CDescriptor{
	token: "test-local-7",
	members: []*SDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:11379"), GetUrl("http://127.0.0.1:11380")},
		{"srv1", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
		{"srv2", GetUrl("http://127.0.0.1:13379"), GetUrl("http://127.0.0.1:13380")},
		{"srv3", GetUrl("http://127.0.0.1:14379"), GetUrl("http://127.0.0.1:14380")},
		{"srv4", GetUrl("http://127.0.0.1:15379"), GetUrl("http://127.0.0.1:15380")},
		{"srv5", GetUrl("http://127.0.0.1:16379"), GetUrl("http://127.0.0.1:16380")},
		{"srv6", GetUrl("http://127.0.0.1:17379"), GetUrl("http://127.0.0.1:17380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}

func MakeUniformCluster(size int, host string) *CDescriptor {
	cluster := &CDescriptor{
		token:  fmt.Sprintf("test-uniform-%d", size),
		lg:     "zap",
		output: embed.StdErrLogOutput,
	}
	cluster.members = make([]*SDescriptor, size)
	for i := 0; i < size; i++ {
		cluster.members[i] = &SDescriptor{
			fmt.Sprintf("srv%d", i),
			GetUrl(fmt.Sprintf("%s:1%d379", host, i+1)),
			GetUrl(fmt.Sprintf("%s:1%d380", host, i+1)),
		}
	}
	return cluster
}

func MakeDistinctCluster(hosts []string) *CDescriptor {
	cluster := &CDescriptor{
		token:  fmt.Sprintf("test-uniform-%d", len(hosts)),
		lg:     "zap",
		output: embed.StdErrLogOutput,
	}
	cluster.members = make([]*SDescriptor, len(hosts))
	for i := 0; i < len(hosts); i++ {
		cluster.members[i] = &SDescriptor{
			fmt.Sprintf("srv%d", i),
			GetUrl(fmt.Sprintf("%s:1%d379", hosts[i], i+1)),
			GetUrl(fmt.Sprintf("%s:1%d380", hosts[i], i+1)),
		}
	}
	return cluster
}

func RemoveMemberUsingEtcdctl(ctl string, rSrv types.ID, tSrv []*SDescriptor) ([]byte, error) {
	s := make([]string, len(tSrv))
	for i, srv := range tSrv {
		s[i] = strings.Join([]string{srv.name, srv.cPort.String()}, "=")
	}
	return exec.Command(ctl, "--endpoints", strings.Join(s, ","), "member", "remove", rSrv.String()).Output()
}

func AddMemberUsingEtcdctl(ctl string, aSrv *SDescriptor, tSrv []*SDescriptor) ([]byte, error) {
	s := make([]string, len(tSrv))
	for i, srv := range tSrv {
		s[i] = strings.Join([]string{srv.name, srv.cPort.String()}, "=")
	}
	return exec.Command(ctl, "--endpoints", strings.Join(s, ","), "member", "add", aSrv.name, aSrv.pPort.String()).Output()
}
