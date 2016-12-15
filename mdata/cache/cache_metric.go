package cache

import (
	"sort"
	"sync"

	"github.com/raintank/metrictank/iter"
	accnt "github.com/raintank/metrictank/mdata/cache/accnt"
)

type CCacheMetric struct {
	sync.RWMutex
	oldest uint32
	newest uint32
	chunks map[uint32]*CacheChunk
	metric string
	accnt  accnt.Accnt
}

func NewCCacheMetric(metric string, accnt accnt.Accnt) *CCacheMetric {
	return &CCacheMetric{
		metric: metric,
		accnt:  accnt,
		chunks: make(map[uint32]*CacheChunk),
	}
}

func (mc *CCacheMetric) Init(prev uint32, itergen iter.IterGen) bool {
	res := mc.Add(prev, itergen)
	if !res {
		return false
	}
	mc.oldest = itergen.Ts()
	mc.newest = itergen.Ts()
	return true
}

func (mc *CCacheMetric) Add(prev uint32, itergen iter.IterGen) bool {
	ts := itergen.Ts()
	endTs := itergen.EndTs()

	mc.Lock()
	defer mc.Unlock()
	if _, ok := mc.chunks[ts]; ok {
		return false
	}

	mc.accnt.Add(mc.metric, ts, itergen.Size())

	mc.chunks[ts] = &CacheChunk{
		ts,
		0,
		prev,
		itergen,
	}

	// if the previous chunk is cached, set this one as it's next
	if _, ok := mc.chunks[prev]; ok {
		mc.chunks[prev].setNext(ts)
	}

	// if the next chunk is cached, set this one as it's previous
	if _, ok := mc.chunks[endTs]; ok {
		mc.chunks[endTs].setPrev(ts)
	}

	// update list head/tail if necessary
	if ts > mc.newest {
		mc.newest = ts
	} else if ts < mc.oldest {
		mc.oldest = ts
	}

	return true
}

// get sorted slice of all chunk timestamps
// assumes we have at least read lock
func (mc *CCacheMetric) sortedTs() *[]uint32 {
	keys := make([]uint32, 0, len(mc.chunks))
	for k := range mc.chunks {
		keys = append(keys, k)
	}
	sort.Sort(sortableUint32(keys))
	return &keys
}

// takes a chunk's ts and returns the length (guessing if necessary)
// assumes we already have at least a read lock
func (mc *CCacheMetric) EndTs(ts uint32) uint32 {
	if (*mc.chunks[ts]).Next == 0 {
		if (*mc.chunks[ts]).Prev == 0 {
			// if a chunk has no next and no previous chunk we have to assume it's length is 0
			return (*mc.chunks[ts]).Ts
		} else {
			// if chunk has no next chunk, but has a previous one, we assume the length of this one is same as the previous one
			return (*mc.chunks[ts]).Ts + ((*mc.chunks[ts]).Ts - (*mc.chunks[(*mc.chunks[ts]).Prev]).Ts)
		}
	} else {
		// if chunk has a next chunk, then the end ts of this chunk is the start ts of the next one
		return (*mc.chunks[(*mc.chunks[ts]).Next]).Ts
	}
}

// assumes we already have at least a read lock
// asc determines the direction of the search, ascending or descending
func (mc *CCacheMetric) seek(ts uint32, keys *[]uint32, asc bool) (uint32, bool) {
	var seekpos int
	var shiftby int

	if asc {
		seekpos = 0
		shiftby = 1
	} else {
		seekpos = len(*keys) - 1
		shiftby = -1
	}

	for {
		if asc {
			if seekpos >= len(*keys) || (*keys)[seekpos] > ts {
				break
			}
		} else {
			if seekpos < 0 || mc.EndTs((*keys)[seekpos]) < ts {
				break
			}
		}

		if (*keys)[seekpos] <= ts && mc.EndTs((*keys)[seekpos]) > ts {
			return (*keys)[seekpos], true
		}

		seekpos = seekpos + shiftby
	}

	return 0, false
}

func (mc *CCacheMetric) Search(from uint32, until uint32) *CCSearchResult {
	mc.RLock()
	defer mc.RUnlock()

	if len(mc.chunks) < 1 {
		return nil
	}

	res := CCSearchResult{
		From:     from,
		Until:    until,
		Start:    make([]iter.IterGen, 0),
		End:      make([]iter.IterGen, 0),
		Complete: false,
	}
	keys := mc.sortedTs()

	ts, ok := mc.seek(from, keys, true)
	if ok {
		// add all consecutive chunks to search results, starting at the one containing "from"
		for ; ts <= (*keys)[len(*keys)-1]; ts = mc.chunks[ts].Next {
			mc.accnt.Hit(mc.metric, ts)

			res.Start = append(res.Start, mc.chunks[ts].Itgen)
			endts := mc.EndTs(ts)
			res.From = endts
			if endts >= until {
				res.Complete = true
				return &res
			}
		}
	}

	ts, ok = mc.seek(until, keys, false)
	if ok {
		for ; ts >= 0 && ts >= res.From; ts = mc.chunks[ts].Prev {
			mc.accnt.Hit(mc.metric, ts)

			res.End = append(res.End, mc.chunks[ts].Itgen)
			fromts := mc.chunks[ts].Ts
			res.Until = fromts
			if fromts < from {
				res.Complete = true
				return &res
			}
		}
	}

	return &res
}
