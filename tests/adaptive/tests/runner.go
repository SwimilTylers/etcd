package tests

import (
	"go.etcd.io/etcd/embed"
)

type TestRunner struct {
	Start   func(c *CDescriptor, idx int) (*embed.Etcd, error)
	Restart func(c *CDescriptor, idx int) (*embed.Etcd, error)
	RunX    func(selected []int, s Scheduler)
	Run1    func(s Scheduler)
	Run3    func(s Scheduler)
	Run5    func(s Scheduler)
	Run7    func(s Scheduler)
}
