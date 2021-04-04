package draft

import (
	"go.etcd.io/etcd/raft/raftpb"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestEmptyUpdate(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	pps, _, _ := preparation(newMockingIMFStorage(), ids, false)
	for _, host := range ids {
		pp := pps[host]
		for _, to := range ids {
			for _, from := range ids {
				if from != host {
					r, f := ft2rf(from, to)
					if !reflect.DeepEqual(pp.GetUpdate(r, f), noUpdate(f)) {
						t.Errorf("host=%v, rack='%s', file='%s': should get no update", host, r, f)
					}
				}
			}
		}
	}
}

func TestVoteUpdate(t *testing.T) {
	ids := []uint64{0, 1, 2, 3, 4}
	pps, mis, mhs := preparation(newMockingIMFStorage(), ids, true)

	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				token := rf2t(ft2rf(from, to))
				mis[token].Vote(1)
				vote := mhs[token].GetTop()
				pp := pps[to]
				for _, other := range ids {
					if other != to {
						r, f := ft2rf(other, to)
						if other == from {
							if !reflect.DeepEqual(pp.GetUpdate(r, f), newUpdate(f, 1, pp.collector[token], 0, vote)) {
								t.Errorf("vote=%v, rack=%s, file=%s: should receive vote", from, r, f)
							}
						} else {
							if !reflect.DeepEqual(pp.GetUpdate(r, f), noUpdate(f)) {
								t.Errorf("vote=%v, rack='%s', file='%s': should get no update", from, r, f)
							}
						}
					}
				}
			}
		}
	}
}

func TestPrimitiveProvider_Write(t *testing.T) {
	type fields struct {
		reader    map[string]*updater
		writer    map[string]IMFWriter
		collector map[string]Collector
	}
	type args struct {
		rack    string
		file    string
		message *raftpb.Message
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvd := &PrimitiveProvider{
				reader:    tt.fields.reader,
				writer:    tt.fields.writer,
				collector: tt.fields.collector,
			}
			if err := pvd.Write(tt.args.rack, tt.args.file, tt.args.message); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func preparation(storage *mockingIMFStorage, ids []uint64, history bool) (map[uint64]*PrimitiveProvider, map[string]*mockingIMFInjector, map[string]*mockingIMFInjectorHistory) {
	pps := make(map[uint64]*PrimitiveProvider)
	mis := make(map[string]*mockingIMFInjector)
	mhs := make(map[string]*mockingIMFInjectorHistory)

	for _, host := range ids {
		pp := NewPrimitiveProvider()
		for _, to := range ids {
			for _, from := range ids {
				r, f := ft2rf(from, to)
				if from == host {
					pp.GrantWrite(r, f, storage.OfferWriteGrant(rf2t(r, f)))
				} else {
					pp.GrantRead(r, f, storage.OfferReadGrant(rf2t(r, f)), NewEntryFragmentCollector(true))
				}
			}
		}
		pps[host] = pp
	}

	for _, from := range ids {
		for _, to := range ids {
			r, f := ft2rf(from, to)
			if history {
				his := &mockingIMFInjectorHistory{}
				mhs[rf2t(r, f)] = his
				mis[rf2t(r, f)] = newMockingMemorableIMFInjector(his).UseMemorable(storage.OfferWriteGrant(rf2t(r, f))).InitAs(0, from, to, 0, 0)
			} else {
				mis[rf2t(r, f)] = newMockingIMFInjector().Use(storage.OfferWriteGrant(rf2t(r, f))).InitAs(0, from, to, 0, 0)
			}

		}
	}

	return pps, mis, mhs
}

func ft2rf(from, to uint64) (rack, file string) {
	file = "F" + strconv.Itoa(int(from))
	rack = "R" + strconv.Itoa(int(to))

	return rack, file
}

func rf2t(rack, file string) (token string) {
	token = filepath.Join(rack, file)
	return token
}

func GenerateRackFileNames(size int) (string, []string, string, []string) {
	lRack := "R0"
	rRacks := make([]string, size-1)
	for i := 0; i < size-1; i++ {
		rRacks[i] = "R" + strconv.Itoa(i+1)
	}

	lFile := "F0"
	rFiles := make([]string, size-1)
	for i := 0; i < size-1; i++ {
		rFiles[i] = "F" + strconv.Itoa(i+1)
	}

	return lRack, rRacks, lFile, rFiles
}

func OfferWriterGrant(r string, f string, wg func(key string) IMFWriter) map[string]IMFWriter {
	res := make(map[string]IMFWriter)
	sig := filepath.Join(r, f)
	res[sig] = wg(sig)

	return res
}

func OfferReaderGrant(r string, fs []string, rg func(key string) IMFReader) map[string]*updater {
	res := make(map[string]*updater)
	for _, f := range fs {
		sig := filepath.Join(r, f)
		res[sig] = &updater{
			next:   0,
			reader: rg(sig),
		}
	}

	return res
}

func OfferCollectors(r string, fs []string, cg func(key string) Collector) map[string]Collector {
	res := make(map[string]Collector)
	for _, f := range fs {
		sig := filepath.Join(r, f)
		res[sig] = cg(sig)
	}

	return res
}

var defaultCollectorGenerator = func(key string) Collector {
	return NewEntryFragmentCollector(true)
}
