package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	v3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/tools/benchtool/data"
	"gopkg.in/cheggaaa/pb.v1"
	"time"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Benchtool verify",

	Run: verifyFunc,
}

var (
	vFile       string
	verifyTotal int
	eDir        string
)

func init() {
	RootCmd.AddCommand(verifyCmd)

	verifyCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")

	verifyCmd.Flags().StringVar(&vFile, "verification-file", "v.dat", "verification file path")
	verifyCmd.Flags().IntVar(&verifyTotal, "total", 10000, "Total number of get requests")
	verifyCmd.Flags().StringVar(&eDir, "error-file-dr", "./test-verify-error", "error file directory")
}

func verifyFunc(cmd *cobra.Command, args []string) {
	stop := time.After(lifetime)
	database := data.GetBenchDataFromString(benchDataDesc)
	clients := mustCreateClients(totalClients, totalConns)

	_ = database.Load(vFile)
	fmt.Println("Verification")

	bar = pb.New(verifyTotal)
	bar.Format("Bom !")
	bar.Start()
	database.InitValidate(verifyTotal, int(totalConns), int(totalClients))
	wg := runClients(clients,
		database.Requests(),
		func(op v3.Op, opResponse v3.OpResponse, err error) {
			if err == nil {
				database.Confirm(opResponse)
			} else {
				database.Error(err)
			}
		},
		nil,
		nil,
	)

	done := make(chan struct{})

	go func() {
		<-time.After(10 * time.Millisecond)
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-stop:
		fmt.Println("=====> bench timeout <=====")
	}

	bar.Finish()
	_ = database.Close()
	fmt.Println(database.Results())
}
