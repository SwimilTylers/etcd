package tests

import "go.etcd.io/etcd/embed"

type NetworkSimulator interface {
	GetOptions() []string
	FindFittestOption(s string) (string, error)

	SetByIP(ip, option string) error
	UnsetByIP(ip, option string) error

	SetByPort(port, option string) error
	UnsetByPort(port, option string) error
}

type SimGen func(option string, descriptor *CDescriptor, id int) func(etcd *embed.Etcd) (*embed.Etcd, error)
