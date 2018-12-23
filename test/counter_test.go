package test

import (
	"testing"
	"github.com/artemirk/counter/counter"
	"github.com/artemirk/counter/utils"
	"sync"
	"time"
)

func Test_Counter(t *testing.T) {
	c, _ := counter.NewContainer()
	var wg sync.WaitGroup

	columns := []string{
		utils.GetRandString(10), 
		utils.GetRandString(10),
		utils.GetRandString(10),
		utils.GetRandString(10),
		utils.GetRandString(10),
	}
	values := [][]string {
		[]string{utils.GetRandString(10), utils.GetRandString(10),},
		[]string{utils.GetRandString(10), utils.GetRandString(10),},
		[]string{utils.GetRandString(10), utils.GetRandString(10),},
		[]string{utils.GetRandString(10), utils.GetRandString(10),},
		[]string{utils.GetRandString(10), utils.GetRandString(10),},
	}

	generateData := func(columns []string, values [][]string) (data map[string]string) {
		data = make(map[string]string, len(columns))
		for i:=0; i < len(columns); i++ {
			data[columns[i]] = values[i][utils.Random(0, len(values[i]))]
		}
		return
	}

	testData := []map[string]string {
		generateData(columns, values),
		generateData(columns, values),
		generateData(columns, values),
		generateData(columns, values),
		generateData(columns, values),
	}

	wg.Add(len(testData))
	for i:=0; i < len(testData); i++ {
		go func(i int) {
			defer wg.Done()
			c.Incr(time.Now().UTC(), testData[i], 1)
		}(i)
	}
	wg.Wait()
}