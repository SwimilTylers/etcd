package tests

import (
	"math/rand"
	"sort"
)

type CrashDesignatorAction uint8

const (
	CDActionNop CrashDesignatorAction = iota
	CDActionCrash
	CDActionRestart
	CDActionFlip
	CDActionStop
)

type ReshuffledDesignator struct {
	perm []int
}

func (dsg *ReshuffledDesignator) MapTo(srv ...int) []int {
	var ret []int
	for _, s := range srv {
		ret = append(ret, dsg.perm[s])
	}
	return ret
}

func NewReshuffleDesignator(s int, pGenerator func(s int) []int) *ReshuffledDesignator {
	return &ReshuffledDesignator{pGenerator(s)}
}

type CrashDesignator interface {
	NextCrash(s int) []int
	NextRestart(s int) []int

	NextAction() (CrashDesignatorAction, []int)
}

type BitonicDesignator struct {
	size int
	cIdx int
	rIdx int

	perm []int
}

func (dsg *BitonicDesignator) NextCrash(s int) []int {
	var ret []int
	for count := 0; count < s; count++ {
		ret = append(ret, dsg.perm[dsg.cIdx])
		dsg.cIdx++
		if dsg.cIdx >= dsg.size {
			break
		}
	}
	return ret
}

func (dsg *BitonicDesignator) NextRestart(s int) []int {
	var ret []int
	for count := 0; count < s; count++ {
		ret = append(ret, dsg.perm[dsg.rIdx])
		dsg.rIdx++
		if dsg.rIdx >= dsg.size {
			break
		}
	}
	return ret
}

func (dsg *BitonicDesignator) NextAction() (CrashDesignatorAction, []int) {
	return CDActionStop, nil
}

func NewBitonicDesignator(size int, pGenerator func(s int) []int) *BitonicDesignator {
	return &BitonicDesignator{
		size: size,
		cIdx: 0,
		rIdx: 0,
		perm: pGenerator(size),
	}
}

var (
	SequentialDesignatorPermGenerator = func(s int) []int {
		perm := make([]int, s)
		for i := 0; i < s; i++ {
			perm[i] = i
		}
		return perm
	}
)

func GetRandomDesignatorPermGenerator(seed rand.Source) func(s int) []int {
	r := rand.New(seed)
	return func(s int) []int {
		return r.Perm(s)
	}
}

type RandomWalkDesignator struct {
	size  int
	async bool
	flip  bool

	survival []bool
	stop     bool

	cumWeight []int

	pendAct CrashDesignatorAction
	pendSrv []int

	r *rand.Rand
}

func (rwd *RandomWalkDesignator) NextCrash(s int) []int {
	perm := rwd.r.Perm(rwd.size)
	count := 0
	var crash []int
	for _, srv := range perm {
		if count >= s {
			break
		} else if rwd.survival[srv] {
			crash = append(crash, srv)
			rwd.survival[srv] = false
			count++
		}
	}
	if len(crash) != 0 {
		for _, sur := range rwd.survival {
			if sur {
				return crash
			}
		}
		unmark := crash[len(crash)-1]
		rwd.survival[unmark] = true
		return crash[:len(crash)-1]
	} else {
		return crash
	}
}

func (rwd *RandomWalkDesignator) NextRestart(s int) []int {
	perm := rwd.r.Perm(rwd.size)
	count := 0
	var restart []int
	for _, srv := range perm {
		if count >= s {
			break
		} else if !rwd.survival[srv] {
			restart = append(restart, srv)
			rwd.survival[srv] = true
			count++
		}
	}
	return restart
}

func (rwd *RandomWalkDesignator) NextAction() (CrashDesignatorAction, []int) {
	if rwd.flip && rwd.pendAct != CDActionNop {
		act, srv := rwd.pendAct, rwd.pendSrv
		rwd.pendAct = CDActionNop
		rwd.pendSrv = nil
		return act, srv
	}

	if rwd.stop {
		return CDActionStop, nil
	} else {
		PrStop := 1.0 / (2.0*float64(rwd.size-1) + 1)
		if rwd.r.Float64() <= PrStop {
			// stop
			rwd.stop = true

			var restart []int
			for idx, sur := range rwd.survival {
				if !sur {
					restart = append(restart, idx)
					rwd.survival[idx] = true
				}
			}

			if len(restart) == 0 {
				return CDActionStop, nil
			} else {
				return CDActionRestart, restart
			}
		} else {
			var srv []int
			var action CrashDesignatorAction

			if !rwd.flip {
				for {
					move := rwd.r.Intn(rwd.cumWeight[1])
					if move < rwd.cumWeight[0] {
						action = CDActionCrash
						if rwd.async {
							srv = rwd.NextCrash(1)
						} else {
							srv = rwd.NextCrash(1 + rwd.r.Intn(rwd.size/2))
						}
					} else {
						action = CDActionRestart
						if rwd.r.Intn(2) < 1 {
							srv = rwd.NextRestart(1)
						} else {
							srv = rwd.NextRestart(1 + rwd.r.Intn(rwd.size))
						}
					}
					if len(srv) != 0 {
						return action, srv
					}
				}
			} else {
				for {
					move := rwd.r.Intn(rwd.cumWeight[2])
					if move < rwd.cumWeight[0] {
						action = CDActionCrash
						if rwd.async {
							srv = rwd.NextCrash(1)
						} else {
							srv = rwd.NextCrash(1 + rwd.r.Intn(rwd.size/2))
						}
					} else if move < rwd.cumWeight[1] {
						action = CDActionRestart
						if rwd.r.Intn(2) < 1 {
							srv = rwd.NextRestart(1)
						} else {
							srv = rwd.NextRestart(1 + rwd.r.Intn(rwd.size))
						}
					} else {
						var cSrv, rSrv []int

						if rwd.async {
							cSrv = rwd.NextCrash(1)
						} else {
							cSrv = rwd.NextCrash(1 + rwd.r.Intn(rwd.size/2))
						}

						if len(cSrv) == 0 {
							continue
						}

						rSrv = rwd.NextRestart(1 + rwd.r.Intn(rwd.size))

						if len(rSrv) == 0 {
							action = CDActionCrash
							srv = cSrv
						} else {
							rwd.pendSrv, rSrv = GetOverlapAndDifference(rSrv, cSrv)
							if len(rwd.pendSrv) > 0 {
								rwd.pendAct = CDActionRestart
							}

							if len(rSrv) == 0 {
								action = CDActionCrash
							} else {
								action = CDActionFlip
							}

							srv = append(cSrv, rSrv...)
						}
					}
					if len(srv) != 0 {
						return action, srv
					}
				}
			}
		}
	}
}

func NewRandomWalkDesignator(size int, async bool, flip bool, seed rand.Source) *RandomWalkDesignator {
	walker := &RandomWalkDesignator{
		size:      size,
		async:     async,
		flip:      flip,
		survival:  make([]bool, size),
		stop:      false,
		cumWeight: ToCumWeight(GlobalRunnerConfigs["rw-weight"].([]int)),
		r:         rand.New(seed),
		pendAct:   CDActionNop,
		pendSrv:   nil,
	}
	for i := 0; i < len(walker.survival); i++ {
		walker.survival[i] = true
	}
	return walker
}

func ToCumWeight(weight []int) []int {
	ret := make([]int, len(weight))
	copy(ret, weight)
	for i := 1; i < len(ret); i++ {
		ret[i] += ret[i-1]
	}
	return ret
}

// GetOverlapAndDifference computes s0 /\ s1 and s0 - s1
func GetOverlapAndDifference(s0, s1 []int) (overlap, difference []int) {
	sort.Ints(s0)
	sort.Ints(s1)

	var needle = 0

	for _, srv0 := range s0 {
		var found = false
		for ; needle < len(s1); needle++ {
			if srv0 == s1[needle] {
				found = true
				overlap = append(overlap, srv0)
				break
			} else if srv0 < s1[needle] {
				break
			}
		}
		if !found {
			difference = append(difference, srv0)
		}
	}

	return overlap, difference
}

var (
	StandardWeights = []int{5, 5, 5}
	FlipMoreWeights = []int{2, 2, 8}
)
