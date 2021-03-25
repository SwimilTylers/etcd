package draft

import "sort"

//CEC2EFCxMerge implements a merging between SimplifiedRaftLogCollector and EntryFragmentCollector.
// The major collector must be a SimplifiedRaftLogCollector. The first minor collector should be a
// SimplifiedRaftLogCollector as well. The rest should be EntryFragmentCollector, omitted if not.
func CEC2EFCxMerge(commit uint64, major Collector, minor []Collector, minorCommit []uint64) uint64 {
	// merge the first minor collector
	var cc = major.(ConsecutiveEntryCollector)
	var mcc = minor[0].(ConsecutiveEntryCollector)

	if !mcc.IsEmpty() && commit <= minorCommit[0] {
		ok, ent, lt, li := mcc.FetchEntriesWithStartIndex(commit)

		if ok {
			_, t := mcc.GetLatestTerm()
			_, tt := cc.GetLatestTerm()

			if cc.IsEmpty() || t >= tt {
				cc.AddEntries(ent, lt, li)
			}
		}

		commit = minorCommit[0]
	}

	// merge the other minor collectors
	var fc []*EntryFragmentCollector
	for i, c := range minor {
		if i == 0 {
			// skip the first minor collector
			continue
		}

		if c.IsEmpty() {
			// skip empty collector
			continue
		}

		if fcc, ok := c.(*EntryFragmentCollector); ok {
			fc = append(fc, fcc)
		}
	}

	// merge fragments
	MergeEntryFragments(commit, fc, cc)

	// merge commit index
	for _, c := range minorCommit {
		if c > commit {
			commit = c
		}
	}

	return commit
}

//MergeEntryFragments merges in-collectors to out-collector.
func MergeEntryFragments(commit uint64, in []*EntryFragmentCollector, out ConsecutiveEntryCollector) {
	inLen := len(in)

	if inLen == 0 {
		return
	}

	fragments := make([]*EntryFragment, 0, inLen)

	_, initTerm := out.GetLatestTerm()

	// draw fragments from in-collector and filter out out-of-date fragments
	for _, c := range in {
		if ok, fragment := c.FetchFragmentsWithStartIndex(commit + 1); ok {
			for j, f := range fragment {
				if f.latestTerm >= initTerm {
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
		if fragments[i].latestTerm == fragments[j].latestTerm {
			return len(fragments[i].fragment) > len(fragments[j].fragment)
		}

		return fragments[i].latestTerm < fragments[j].latestTerm
	})

	// merging fragments into out-collector
	nextTerm := fragments[0].latestTerm

	for _, f := range fragments {
		if f.latestTerm >= nextTerm {
			out.AddEntries(f.fragment, f.logTerm, f.logIndex)
			nextTerm++
		}
	}
}
