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
)

func init() {
	RootCmd.AddCommand(verifyCmd)
	verifyCmd.Flags().StringVar(&vFile, "verification-file", "v.dat", "verification file path")
	verifyCmd.Flags().IntVar(&verifyTotal, "total", 10000, "Total number of get requests")
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
		func(op v3.Op, opResponse v3.OpResponse) { database.Confirm() <- opResponse },
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
	fmt.Println(database.Results())
}
