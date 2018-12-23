package counter

import (
	"github.com/artemirk/counter/utils"
	"time"
	"sync"
)

const (
	DaySeconds = 24 * 60 * 60
)

type Container struct {
	columns map[string]uint32
	values map[uint32]map[string]uint32
	m *sync.Mutex
	cubes map[int64]*Cube
}


func NewContainer() (*Container, error) {
	c := &Container {
		columns: make(map[string]uint32, 100),
		values: make(map[uint32]map[string]uint32, 10000),
		m: &sync.Mutex{}, 
		cubes: make(map[int64]*Cube, 1),
	}

	return c, nil
}


func (c *Container) indexes(data map[string]string) map[uint32]uint32 {
	indexes := make(map[uint32]uint32, len(data))
	for k, v := range data {
		_, ok1 := c.columns[k]
		if !ok1{
			c.columns[k] = uint32(len(c.columns))
		}
		_, ok2 := c.values[c.columns[k]]
		if !ok2{
			c.values[c.columns[k]] = make(map[string]uint32, len(data))
		}
		_, ok3 := c.values[c.columns[k]][v]
		if !ok3{
			c.values[c.columns[k]][v] = uint32(len(c.values[c.columns[k]]))
		}
		indexes[c.columns[k]] = c.values[c.columns[k]][v]
	}
	return indexes
}


func (c *Container) Incr(t time.Time, data map[string]string, count uint64) {
	c.m.Lock()
	indexes := c.indexes(data)
	d, m, _  := utils.GetSlots(t)
	if _, ok := c.cubes[d]; !ok {
		c.cubes[d] = NewCube(t)
	}
	c.m.Unlock()
	c.cubes[d].IncrementBy(m, indexes, count)

}