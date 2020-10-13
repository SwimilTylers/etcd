package utils

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

type NetemKVs struct {
	K      string `yaml:"key"`
	V      string `yaml:"val"`
	FlowId string `yaml:"flow-id"`
}

type TC struct {
	InitShell  string     `yaml:"init_shell"`
	ClearShell string     `yaml:"clear_shell"`
	Device     string     `yaml:"tc_dev"`
	Netem      []NetemKVs `yaml:"netem"`
}

func (t *TC) GetOptions() []string {
	ret := make([]string, len(t.Netem))
	for i, kVs := range t.Netem {
		ret[i] = kVs.K
	}
	return ret
}

func (t *TC) FindFittestOption(s string) (string, error) {
	s = strings.ToUpper(s)
	options := t.GetOptions()

	var candidate string
	var candidateLevel = -1

	for _, option := range options {
		uOption := strings.ToUpper(option)
		if uOption == s {
			return option, nil
		} else if strings.HasPrefix(uOption, s) || strings.HasSuffix(uOption, s) {
			if candidateLevel < 2 {
				candidate = option
				candidateLevel = 2
			}
		} else if strings.Contains(uOption, s) {
			if candidateLevel < 1 {
				candidate = option
				candidateLevel = 1
			}
		} else if strings.Contains(s, uOption) {
			if candidateLevel < 0 {
				candidate = option
				candidateLevel = 0
			}
		}
	}

	if candidateLevel > -1 {
		return candidate, nil
	} else {
		return "", errors.New("cannot find fittest option")
	}
}

func (t *TC) SetByIP(ip, option string) error {
	for _, net := range t.Netem {
		if net.K == option {
			parent := strings.Join([]string{strings.Split(net.FlowId, ":")[0], ":0"}, "")
			cStr := fmt.Sprintf("tc filter add dev %s parent %s protocol ip u32 match ip src %s flowid %s",
				t.Device, parent, ip, net.FlowId)

			cmd := exec.Command("sudo", strings.Split(cStr, " ")...)
			return cmd.Run()
		}
	}

	return errors.New("cannot find the option")
}

func (t *TC) UnsetByIP(ip, option string) error {
	for _, net := range t.Netem {
		if net.K == option {
			parent := strings.Join([]string{strings.Split(net.FlowId, ":")[0], ":0"}, "")
			cStr := fmt.Sprintf("tc filter del dev %s parent %s protocol ip u32 match ip src %s flowid %s",
				t.Device, parent, ip, net.FlowId)

			cmd := exec.Command("sudo", strings.Split(cStr, " ")...)
			return cmd.Run()
		}
	}

	return errors.New("cannot find the option")
}

func (t *TC) SetByPort(port string, option string) error {
	for _, net := range t.Netem {
		if net.K == option {
			parent := strings.Join([]string{strings.Split(net.FlowId, ":")[0], ":0"}, "")
			cStr := fmt.Sprintf("tc filter add dev %s parent %s protocol ip u32 match ip sport %s 0xffff flowid %s",
				t.Device, parent, port, net.FlowId)

			cmd := exec.Command("sudo", strings.Split(cStr, " ")...)
			return cmd.Run()
		}
	}

	return errors.New("cannot find the option")
}

func (t *TC) UnsetByPort(port string, option string) error {
	for _, net := range t.Netem {
		if net.K == option {
			parent := strings.Join([]string{strings.Split(net.FlowId, ":")[0], ":0"}, "")
			cStr := fmt.Sprintf("tc filter del dev %s parent %s protocol ip u32 match ip sport %s 0xffff flowid %s",
				t.Device, parent, port, net.FlowId)

			cmd := exec.Command("sudo", strings.Split(cStr, " ")...)
			return cmd.Run()
		}
	}

	return errors.New("cannot find the option")
}

func GetTCFromYaml(file string) (*TC, error) {
	if b, err := ioutil.ReadFile(file); err != nil {
		return nil, err
	} else {
		tc := new(TC)
		if err := yaml.Unmarshal(b, tc); err != nil {
			return nil, err
		} else {
			return tc, nil
		}
	}
}

func (t *TC) Init() error {
	if err := os.Setenv("TC_DEV", t.Device); err != nil {
		return errors.New(fmt.Sprintf("failed to set TC_DEV=%s", t.Device))
	}

	for _, kVs := range t.Netem {
		if err := os.Setenv(kVs.K, kVs.V); err != nil {
			return errors.New(fmt.Sprintf("failed to set %s=%s", kVs.K, kVs.V))
		}
	}

	return exec.Command("bash", t.InitShell).Run()
}

func (t *TC) Clear() {
	exec.Command("bash", t.ClearShell).Run()

	os.Unsetenv("TC_DEV")

	for _, kVs := range t.Netem {
		os.Unsetenv(kVs.K)
	}
}
