package counter

import (
	"github.com/artemirk/counter/utils"
	"sync/atomic"
	"unsafe"
	"sync"
	"time"
	"fmt"
)

const (
	DayMinutes = 24 * 60
	NumPartitions = 64
)

// Counter is the list of values for columns and values
type Counter struct {
	iHash uint64
	iData map[uint32]uint32
	count uint64
	next *Counter
}

func (c *Counter) Count() uint64 {
	return atomic.LoadUint64(&c.count)
}

// Create new counter for given iColumn/ iValue and count
func NewCounter(iData map[uint32]uint32, count uint64) *Counter {
	iHash, _ := utils.HashMapUint32(iData)
	return &Counter {
		iHash: iHash,
		iData: iData,
		count: count,
	}
	
}

func(c *Counter) IncrementBy(c2 *Counter) uint64 {
	counter := c
	counterPrev := c
	for counter != nil {
		// To speed up compare the hash first
		if counter.iHash == c2.iHash {
			return atomic.AddUint64(&counter.count, c2.count)
		}
		counterPrev = counter
		counter = counter.next
	}
	
	// Toggle pointer and perform updating if swap got failed
	next := (*unsafe.Pointer)(unsafe.Pointer(&counterPrev.next))
	if !atomic.CompareAndSwapPointer(next, unsafe.Pointer(nil), unsafe.Pointer(c2)) {
		return counterPrev.next.IncrementBy(c2)
	}
	return c2.count
}

func(c *Counter) Get(groupBy map[uint32]struct{}, filter map[uint32]map[uint32]struct{}, countes map[uint64]uint64) {

	if countes == nil {
		countes = make(map[uint64]uint64, len(groupBy))
	}

	counter := c
	m := make(map[uint32]struct{}, len(groupBy))
	for counter != nil {
		filtered := true
		for f, fv := range filter {
			if v, ok := counter.iData[f]; ok {
				if _, ok := fv[v]; ok {
					continue
				}
			}
			filtered = false
			break
		}
		if !filtered {
			counter = counter.next
			continue
		}

		for group := range groupBy {
			if _, ok := counter.iData[group]; ok {
				m[group]=struct{}{}
			}
		}

		if len(m) == 0 {
			counter = counter.next
			continue
		}

		iHash, _ := utils.HashKeysUint32(m)
		if _, ok := countes[iHash]; !ok { 
			countes[iHash] = counter.Count() 
		} else { 
			countes[iHash] += counter.Count() 
		}
		counter = counter.next
		m = make(map[uint32]struct{}, len(groupBy))
	}
}

type Cube struct {
	start int64
	partitions [DayMinutes][NumPartitions]*Counter
	logger *utils.Logger
}


func NewCube(t time.Time) *Cube {
	start, _, _ := utils.GetSlots(t)
	c := &Cube{
		start: start, 
		logger: utils.GetLogger(utils.Debug),
	}
	return c
}


func (c *Cube) IncrementBy(m int, iData map[uint32]uint32, count uint64) (uint64, error)  {
	if m >= DayMinutes {
		return 0, fmt.Errorf("Incorrect cube minute: %v expected %v", m, DayMinutes-1)
	}

	counter := NewCounter(iData, count)
	iHash, _ := utils.HashMapUint32(iData)
	slot := utils.Range(iHash)
	c.logger.Debug.Printf("m=%v slot=%v\n", m, slot)
	data := (*unsafe.Pointer)(unsafe.Pointer(&(c.partitions[m][slot])))
	c.logger.Debug.Printf("counter=%v, iHash=%v, slot=%v, data=%v\n", counter, iHash, slot, c.partitions[m][slot])
	if !atomic.CompareAndSwapPointer(data, unsafe.Pointer(nil), unsafe.Pointer(counter)) {
		return c.partitions[m][slot].IncrementBy(counter), nil
	}
	return count, nil
}


func (c *Cube) Get(
	groupBy map[uint32]struct{}, 
	filter map[uint32]map[uint32]struct{}, 
	start *time.Time, 
	end *time.Time,
) map[uint64]uint64 {
	iStart := 0
	iEnd := DayMinutes-1
	if start != nil {
		slotStart, m, _ := utils.GetSlots(*start)
		if c.start == slotStart {
			iStart = m
		} else {
			return nil
		}
	}
	if end != nil {
		slotEnd, m, _ := utils.GetSlots(*end)
		if c.start == slotEnd {
			iEnd = m
		}
	}

	counters := make(map[uint64]uint64, len(groupBy))

	var wg sync.WaitGroup
	wg.Add(65)
	countersCh := make(chan map[uint64]uint64)

	go func() {
		defer wg.Done()
		n := 0
		for c := range countersCh {
			for k, v := range c {
				if _, ok := counters[k]; !ok { 
					counters[k] = v 
				} else { 
					counters[k] += v
				}
			}
			n++
			if n == (NumPartitions-1) { break }
		}
	}()

	for s := 0; s < NumPartitions; s++ {
		go func(i int) {
			defer wg.Done()
			cc := make(map[uint64]uint64, len(groupBy))
			for m := iStart; m<=iEnd; m++ {
				c.partitions[m][i].Get(groupBy, filter, cc)
			}
			countersCh <- cc
		}(s)
	}

	wg.Done()
	
	return counters
}
