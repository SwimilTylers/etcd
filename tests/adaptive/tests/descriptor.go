package tests

import (
	"go.etcd.io/etcd/embed"
	"net/url"
	"strings"
)

type sDescriptor struct {
	name  string
	cPort *url.URL
	pPort *url.URL
}

type cDescriptor struct {
	token   string
	members []*sDescriptor

	lg     string
	output string
}

func (c *cDescriptor) Cluster() string {
	var b strings.Builder
	b.WriteString(c.members[0].name + "=" + c.members[0].pPort.String())
	for i := 1; i < len(c.members); i++ {
		b.WriteString("," + c.members[i].name + "=" + c.members[i].pPort.String())
	}
	return b.String()
}

func (c *cDescriptor) GetConfig(idx int, clusterState string) *embed.Config {
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
		cfg.LogOutputs = []string{c.output}
	}

	return cfg
}

func GetUrl(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

var DefaultLocalCluster1 = &cDescriptor{
	token: "test-local-1",
	members: []*sDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}

var DefaultLocalCluster3 = &cDescriptor{
	token: "test-local-3",
	members: []*sDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
		{"srv1", GetUrl("http://127.0.0.1:22379"), GetUrl("http://127.0.0.1:22380")},
		{"srv2", GetUrl("http://127.0.0.1:32379"), GetUrl("http://127.0.0.1:32380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}

var DefaultLocalCluster5 = &cDescriptor{
	token: "test-local-5",
	members: []*sDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
		{"srv1", GetUrl("http://127.0.0.1:22379"), GetUrl("http://127.0.0.1:22380")},
		{"srv2", GetUrl("http://127.0.0.1:32379"), GetUrl("http://127.0.0.1:32380")},
		{"srv3", GetUrl("http://127.0.0.1:42379"), GetUrl("http://127.0.0.1:42380")},
		{"srv4", GetUrl("http://127.0.0.1:52379"), GetUrl("http://127.0.0.1:52380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}

var DefaultLocalCluster7 = &cDescriptor{
	token: "test-local-7",
	members: []*sDescriptor{
		{"srv0", GetUrl("http://127.0.0.1:12379"), GetUrl("http://127.0.0.1:12380")},
		{"srv1", GetUrl("http://127.0.0.1:22379"), GetUrl("http://127.0.0.1:22380")},
		{"srv2", GetUrl("http://127.0.0.1:32379"), GetUrl("http://127.0.0.1:32380")},
		{"srv3", GetUrl("http://127.0.0.1:42379"), GetUrl("http://127.0.0.1:42380")},
		{"srv4", GetUrl("http://127.0.0.1:52379"), GetUrl("http://127.0.0.1:52380")},
		{"srv5", GetUrl("http://127.0.0.1:62379"), GetUrl("http://127.0.0.1:62380")},
		{"srv6", GetUrl("http://127.0.0.1:72379"), GetUrl("http://127.0.0.1:72380")},
	},
	lg:     "zap",
	output: embed.StdErrLogOutput,
}
