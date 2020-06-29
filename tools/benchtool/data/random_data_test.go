package data

import (
	"encoding/binary"
	"go.etcd.io/etcd/clientv3"
	"math"
	"reflect"
	"testing"
)

func TestParseRandomDataOptions(t *testing.T) {
	type args struct {
		opts string
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
	}{
		{"default config", args{""}, GetDefaultRandomDataOptions()},
		{"default config (with space)", args{" "}, GetDefaultRandomDataOptions()},
		{"change mode", args{"[mode=some_type]"}, func() map[string]interface{} { c := GetDefaultRandomDataOptions(); c["mode"] = "some_type"; return c }()},
		{"change key-size", args{"[key-size=16]"}, func() map[string]interface{} { c := GetDefaultRandomDataOptions(); c["key-size"] = 16; return c }()},
		{"change val-size", args{"[val-size=16]"}, func() map[string]interface{} { c := GetDefaultRandomDataOptions(); c["val-size"] = 16; return c }()},
		{"change all opts", args{"[mode=some_type,key-size=16,val-size=16]"}, func() map[string]interface{} {
			c := GetDefaultRandomDataOptions()
			c["mode"] = "some_type"
			c["key-size"] = 16
			c["val-size"] = 16
			return c
		}()},
		{"change all opts (with space)", args{" [mode=some_type,key-size=16,val-size=16] "}, func() map[string]interface{} {
			c := GetDefaultRandomDataOptions()
			c["mode"] = "some_type"
			c["key-size"] = 16
			c["val-size"] = 16
			return c
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseRandomDataOptions(tt.args.opts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseRandomDataOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryEncodingAndDecoding(t *testing.T) {
	k := make([]byte, 8)
	for i := 0; i < math.MaxInt32; i++ {
		// copy(k, strconv.Itoa(i))
		binary.PutVarint(k, int64(i))
		op := clientv3.OpPut(string(k), "")
		// j, _ := strconv.Atoi(string(op.KeyBytes()))
		j, _ := binary.Varint(op.KeyBytes())
		if int64(i) != j {
			t.Fatal("when i =", int64(i), "failed to Encode/Decode [ i' =", j, "]")
		}
	}
}
