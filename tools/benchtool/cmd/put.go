// Copyright 2020 swimiltylers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/tools/benchtool/data"
	"math"
	"sync"
	"time"

	v3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/report"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
)

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Benchtool put",

	Run: putFunc,
}

var (
	putTotal int
	putRate  int

	compactInterval   time.Duration
	compactIndexDelta int64

	requestWait time.Duration
	verifyWait  time.Duration
)

func init() {
	RootCmd.AddCommand(putCmd)
	putCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")

	putCmd.Flags().IntVar(&putTotal, "total", 10000, "Total number of put requests")
	putCmd.Flags().DurationVar(&compactInterval, "compact-interval", 0, `Interval to compact database (do not duplicate this with etcd's 'auto-compaction-retention' flag) (e.g. --compact-interval=5m compacts every 5-minute)`)
	putCmd.Flags().Int64Var(&compactIndexDelta, "compact-index-delta", 1000, "Delta between current revision and compact revision (e.g. current revision 10000, compact at 9000)")

	putCmd.Flags().DurationVar(&requestWait, "request-wait-timeout", 150*time.Millisecond, "")
	putCmd.Flags().DurationVar(&verifyWait, "verify-wait-timeout", 5*time.Second, "")
}

func putFunc(cmd *cobra.Command, args []string) {
	stop := time.After(lifetime)
	database := data.GetBenchDataFromString(benchDataDesc)

	database.Init(putTotal, int(totalConns), int(totalClients))

	if putRate == 0 {
		putRate = math.MaxInt32
	}
	clients := mustCreateClients(totalClients, totalConns)

	bar = pb.New(putTotal)
	bar.Format("Bom !")
	bar.Start()

	r := newReport()

	wg := runClients(clients,
		database.Requests(),
		func(op v3.Op, opResponse v3.OpResponse) {
			database.Acknowledge() <- struct {
				v3.Op
				v3.OpResponse
			}{op, opResponse}
		},
		rate.NewLimiter(rate.Limit(putRate), 1),
		r,
	)

	if compactInterval > 0 {
		go func() {
			for {
				time.Sleep(compactInterval)
				compactKV(clients)
			}
		}()
	}

	rc := r.Run()
	done := make(chan struct{})

	go func() {
		wg.Wait()
		bar.Finish()
		<-time.After(verifyWait)
		fmt.Println("Verifying now:")
		bar = pb.New(putTotal)
		bar.Format("Bom !")
		bar.Start()
		database.InitValidate(putTotal, int(totalConns), int(totalClients))
		wg = runClients(clients,
			database.Requests(),
			func(op v3.Op, opResponse v3.OpResponse) { database.Confirm() <- opResponse },
			nil,
			nil,
		)
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-stop:
		fmt.Println("=====> bench timeout <=====")
	}
	close(r.Results())
	bar.Finish()
	fmt.Println(<-rc)
	fmt.Println(database.Results())
}

func compactKV(clients []*v3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := clients[0].KV.Get(ctx, "foo")
	cancel()
	if err != nil {
		panic(err)
	}
	revToCompact := max(0, resp.Header.Revision-compactIndexDelta)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = clients[0].KV.Compact(ctx, revToCompact)
	cancel()
	if err != nil {
		panic(err)
	}
}

func runClients(clients []*v3.Client, requests <-chan v3.Op, response func(op v3.Op, opResponse v3.OpResponse), limit *rate.Limiter, r report.Report) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	for i := range clients {
		wg.Add(1)
		go func(c *v3.Client) {
			defer wg.Done()

			for op := range requests {
				if limit != nil {
					limit.Wait(context.Background())
				}

				firstRequest := true

				func() {
					var ctx context.Context
					if firstRequest {
						ctx = context.Background()
						firstRequest = false
					} else {
						var cancel context.CancelFunc
						ctx, cancel = context.WithTimeout(context.Background(), requestWait)
						defer cancel()
					}

					st := time.Now()
					resp, err := c.Do(ctx, op)

					if r != nil {
						r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
					}

					if err == nil {
						response(op, resp)
					}
				}()

				bar.Increment()
			}
		}(clients[i])
	}

	return wg
}

func max(n1, n2 int64) int64 {
	if n1 > n2 {
		return n1
	}
	return n2
}
