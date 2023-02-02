package csvutil

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
)

type Reducer struct {
	limit int

	// stat
	required []string
	values   []float64
	stats    map[string]float64

	// channel
	out io.Writer
}

func newReducer() *Reducer {
	return &Reducer{}
}

func newStatReducer(required_stats []string) *Reducer {
	reducer := &Reducer{
		stats:    make(map[string]float64),
		required: required_stats,
	}
	reducer.stats["min"] = math.MaxInt64
	reducer.stats["max"] = math.MinInt64
	reducer.stats["nulls"] = 0
	return reducer
}

func newColumnsReducer(fd io.Writer, limit int) *Reducer {
	return &Reducer{
		out:   fd,
		limit: limit,
	}
}

func (reducer *Reducer) reduceCount(mappers []*Mapper, mode string, wg *sync.WaitGroup) map[string]int64 {
	wg.Wait()
	var result map[string]int64 = make(map[string]int64)
	switch mode {
	case "lines", "bytes":
		for _, mapper := range mappers {
			result["total"] += mapper.getCount()["total"]
		}

	case "group":
		for _, mapper := range mappers {
			for key, value := range mapper.getCount() {
				result[key] += value
			}
		}
	}
	return result
}

func (reducer *Reducer) reduceStat(channel chan string, wg *sync.WaitGroup) (map[string]float64, error) {
	waitCh := make(chan int)
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	for {
		select {
		case data := <-channel:
			value, err := strconv.ParseFloat(data, 64)
			if data == "" {
				reducer.stats["nulls"]++
			} else {
				reducer.stats["count"]++
			}

			if err != nil && data != "" {
				return nil, fmt.Errorf("value provided is not numeric: %s", data)
			}

			if value > reducer.stats["max"] {
				reducer.stats["max"] = value
			}
			if value < reducer.stats["min"] {
				reducer.stats["min"] = value
			}
			reducer.stats["sum"] += value

			// avoid calculating if not necessary; to save space
			if ExistsIn("std_dev", reducer.required) {
				reducer.values = append(reducer.values, value)
			}
		case <-waitCh:
			close(channel)
			reducer.stats["mean"] = reducer.stats["sum"] / reducer.stats["count"]

			// avoid calculating if not necessary to save space
			if ExistsIn("std_dev", reducer.required) {
				var variance float64
				for _, value := range reducer.values {
					variance += math.Pow(reducer.stats["mean"]-value, 2)
				}
				reducer.stats["std_dev"] = math.Sqrt(variance / reducer.stats["sum"])
			}
			return reducer.stats, nil
		}
	}
}

func (reducer *Reducer) reduceColumns(channel chan string, wg *sync.WaitGroup) {
	waitCh := make(chan int)
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	for {
		select {
		case <-waitCh:
			return
		case line := <-channel:
			if reducer.limit > 0 {
				reducer.out.Write([]byte(line))
				reducer.limit--
			} else if reducer.limit == -1 {
				reducer.out.Write([]byte(line))
			} else {
				return
			}
		}
	}
}
