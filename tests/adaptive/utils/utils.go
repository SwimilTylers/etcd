package utils

import (
	"fmt"
	"io/ioutil"
	"os"
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
