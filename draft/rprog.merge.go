package draft

import (
	"go.etcd.io/etcd/draft/collector"
	"sort"
)

//CECEFCxMerge implements a merging between MimicRaftKernelCollector and MultiFragmentsCollector.
// The major collector must be a MimicRaftKernelCollector. The first minor collector should be a
// SingleFragmentCollector as well. The rest should be MultiFragmentsCollector, omitted if not.
func CECEFCxMerge(commit uint64, major collector.Collector, minor []collector.EntryFragmentCollector, minorCommit []uint64) uint64 {
	// merge the first minor collector
	var cc = major.(collector.ConsecutiveEntryCollector)

	// merge fragments
	MergeEntryFragments(commit, minor, cc)

	// merge commit index
	for _, c := range minorCommit {
		if c > commit {
			commit = c
		}
	}

	return commit
}

//MergeEntryFragments merges in-collectors to out-collector.
func MergeEntryFragments(commit uint64, in []collector.EntryFragmentCollector, out collector.ConsecutiveEntryCollector) {
	inLen := len(in)

	if inLen == 0 {
		return
	}

	fragments := make([]*collector.EntryFragment, 0, inLen)

	_, initTerm := out.GetLatestTerm()

	// draw fragments from in-collector and filter out out-of-date fragments
	for _, c := range in {
		if ok, fragment := c.FetchFragmentsWithStartIndex(commit + 1); ok {
			for j, f := range fragment {
				if f.CTerm >= initTerm {
					fragments = append(fragments, fragment[j:]...)
					break
				}
			}
		}
	}

	if len(fragments) == 0 {
		return
	}

	// sort the fragments
	sort.Slice(fragments, func(i, j int) bool {
		if fragments[i].CTerm == fragments[j].CTerm {
			return len(fragments[i].Fragment) > len(fragments[j].Fragment)
		}

		return fragments[i].CTerm < fragments[j].CTerm
	})

	// merging fragments into out-collector
	nextTerm := fragments[0].CTerm

	for _, f := range fragments {
		if f.CTerm >= nextTerm {
			out.AddEntries(f.Fragment, f.LogTerm, f.LogIndex)
			nextTerm++
		}
	}
}
