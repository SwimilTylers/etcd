package tests

import "math/rand"

type CrashDesignatorAction uint8

const (
	CDActionNop CrashDesignatorAction = iota
	CDActionCrash
	CDActionRestart
	CDActionStop
)

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

func NewDesignator(size int, pGenerator func(s int) []int) *BitonicDesignator {
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

	survival []bool
	stop     bool

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

			for {
				move := rwd.r.Intn(2)
				if move < 1 {
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
		}
	}
}

func NewRandomWalkDesignator(size int, async bool, seed rand.Source) *RandomWalkDesignator {
	walker := &RandomWalkDesignator{
		size:     size,
		async:    async,
		survival: make([]bool, size),
		stop:     false,
		r:        rand.New(seed),
	}
	for i := 0; i < len(walker.survival); i++ {
		walker.survival[i] = true
	}
	return walker
}
