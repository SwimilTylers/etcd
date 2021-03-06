package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func RemoveAllSrvInfo() error {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() && strings.Contains(file.Name(), ".srv") {
			if err := os.RemoveAll(file.Name()); err != nil {
				fmt.Println(file.Name(), ": remove failed")
				return err
			} else {
				fmt.Println(file.Name(), ": remove success")
			}
		}
	}

	return nil
}

func RemoveAllSrvLog() error {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "etcd-") && strings.HasSuffix(file.Name(), ".log") {
			if err := os.Remove(file.Name()); err != nil {
				fmt.Println(file.Name(), ": remove failed")
				return err
			} else {
				fmt.Println(file.Name(), ": remove success")
			}
		}
	}

	return nil
}

func RemoveAllSchLog() error {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), "sch.log") {
			if err := os.Remove(file.Name()); err != nil {
				fmt.Println(file.Name(), ": remove failed")
				return err
			} else {
				fmt.Println(file.Name(), ": remove success")
			}
		}
	}

	return nil
}

func ExtractArgs(args string, subCmd string) []map[string]string {
	dicts := []map[string]string{make(map[string]string)}
	if subCmd != "" {
		dicts = append(dicts, make(map[string]string))
		for i, s := range strings.Split(args, subCmd) {
			kvs := strings.Split(s, "--")
			for _, kv := range kvs {
				if len(kv) > 0 {
					if strings.Contains(kv, "=") {
						skv := strings.SplitN(kv, "=", 2)
						dicts[i][skv[0]] = skv[1]
					} else if kv != " " {
						dicts[i][kv] = ""
					}
				}
			}
		}
	} else {
		kvs := strings.Split(args, "--")
		for _, kv := range kvs {
			if len(kv) > 0 {
				if strings.Contains(kv, "=") {
					skv := strings.Split(kv, "=")
					dicts[0][skv[0]] = skv[1]
				} else {
					dicts[0][kv] = ""
				}
			}
		}
	}
	return dicts
}

func MakeArgs(dicts []map[string]string, subCmd string) string {
	b := strings.Builder{}
	for k, v := range dicts[0] {
		if v == "" {
			b.WriteString(fmt.Sprintf("--%s ", strings.TrimSpace(k)))
		} else {
			b.WriteString(fmt.Sprintf("--%s=%s ", strings.TrimSpace(k), strings.TrimSpace(v)))
		}
	}
	if subCmd != "" && len(dicts) >= 2 {
		b.WriteString(subCmd)
		for k, v := range dicts[1] {
			if v == "" {
				b.WriteString(fmt.Sprintf(" --%s", strings.TrimSpace(k)))
			} else {
				b.WriteString(fmt.Sprintf(" --%s=%s", strings.TrimSpace(k), strings.TrimSpace(v)))
			}
		}
	}
	return b.String()
}

func FetchAndGenerateExecArgs(supplement ...string) (string, []string) {
	args := os.Args
	args = append(args, supplement...)
	return args[0], args
}

func ReadSelected(s string) []int {
	if s == "all" {
		return nil
	} else {
		var ret []int
		slice := strings.Split(strings.Trim(s, "[ ]"), ", ")
		for _, k := range slice {
			if t, err := strconv.Atoi(k); err == nil {
				ret = append(ret, t)
			}
		}
		return ret
	}
}
