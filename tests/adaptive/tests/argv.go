package tests

import (
	"os"
	"strings"
)

type ArgvBuilder struct {
	argv0  string
	argv   map[string]string
	flags  map[string]string
	bFlags map[string]bool
	remove map[string]struct{}
}

func (ab *ArgvBuilder) String() string {
	b := strings.Builder{}
	b.WriteString(ab.argv0)
	b.WriteString(" ")
	for k, v := range ab.argv {
		b.WriteString(k)
		b.WriteString(" ")
		if v != "" {
			b.WriteString(v)
			b.WriteString(" ")
		}
	}
	return b.String()
}

func (ab *ArgvBuilder) StringSlice() []string {
	var ret = []string{ab.argv0}
	for k, v := range ab.argv {
		if v != "" {
			ret = append(ret, k, v)
		} else {
			ret = append(ret, k)
		}
	}
	return ret
}

func (ab *ArgvBuilder) CopyOf() *ArgvBuilder {
	_argv := make(map[string]string)
	for k, v := range ab.argv {
		_argv[k] = v
	}
	return &ArgvBuilder{
		argv0:  ab.argv0,
		argv:   _argv,
		flags:  make(map[string]string),
		bFlags: make(map[string]bool),
		remove: make(map[string]struct{}),
	}
}

func (ab *ArgvBuilder) AddFlag(k string, v string) *ArgvBuilder {
	ab.flags[k] = v
	return ab
}

func (ab *ArgvBuilder) AddBoolFlag(k string, v bool) *ArgvBuilder {
	ab.bFlags[k] = v
	return ab
}

func (ab *ArgvBuilder) RemoveFlag(k string) *ArgvBuilder {
	ab.remove[k] = struct{}{}
	return ab
}

func (ab *ArgvBuilder) Align() *ArgvBuilder {
	for k, _ := range ab.argv {
		k = strings.Split(k, "=")[0]
		if _, ok := ab.flags[k]; ok {
			delete(ab.argv, k)
		} else if _, ok := ab.bFlags[k]; ok {
			delete(ab.argv, k)
		} else if _, ok := ab.remove[k]; ok {
			delete(ab.argv, k)
		}
	}
	for k, v := range ab.flags {
		ab.argv[k] = v
	}
	for k, is := range ab.bFlags {
		if is {
			ab.argv[k] = ""
		} else {
			ab.argv[strings.Join([]string{k, "false"}, "=")] = ""
		}
	}
	return ab
}

func InitRootArgvBuilder() {
	argv := os.Args
	builder := &ArgvBuilder{argv0: argv[0], argv: make(map[string]string)}
	var last = ""
	var start int
	for i, arg := range argv[1:] {
		if strings.HasPrefix(arg, "-") {
			if _, ok := builder.argv[strings.Split(last, "=")[0]]; !ok && last != "" {
				builder.argv[last] = strings.Join(argv[1+start:1+i], " ")
			}
			last = arg
			start = i + 1
		}
	}
	if _, ok := builder.argv[strings.Split(last, "=")[0]]; !ok && 1+start <= len(argv) && last != "" {
		builder.argv[last] = strings.Join(argv[1+start:], " ")
	}
	GlobalRunnerConfigs["root-argv-builder"] = builder
}
