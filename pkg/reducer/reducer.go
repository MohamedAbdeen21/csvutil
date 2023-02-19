package reducer

import (
	"github.com/MohamedAbdeen21/csvutil/pkg/mapper"
	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
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

func NewReducer() *Reducer {
	return &Reducer{}
}

func NewStatReducer(required_stats []string) *Reducer {
	reducer := &Reducer{
		stats:    make(map[string]float64),
		required: required_stats,
	}
	reducer.stats["min"] = math.MaxInt64
	reducer.stats["max"] = math.MinInt64
	reducer.stats["nulls"] = 0
	return reducer
}

func NewColumnsReducer(fd io.Writer, limit int) *Reducer {
	return &Reducer{
		out:   fd,
		limit: limit,
	}
}

func (reducer *Reducer) ReduceCount(mappers []*mapper.Mapper, mode string, wg *sync.WaitGroup) map[string]int64 {
	wg.Wait()
	var result map[string]int64 = make(map[string]int64)
	switch mode {
	case "lines":
		for _, m := range mappers {
			result["total"] += m.GetLinesCount()
		}
	case "bytes":
		for _, m := range mappers {
			result["total"] += m.GetBytesCount()
		}
	case "group":
		for _, m := range mappers {
			for key, value := range m.GetGroupCount() {
				result[key] += value
			}
		}
	}
	return result
}

func (reducer *Reducer) ReduceStat(channel chan string, wg *sync.WaitGroup) map[string]float64 {
	waitCh := make(chan int)
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(waitCh)
	}(wg)

	is_numerical_column := false
	if utility.ListExsistsIn([]string{"max", "min", "mean", "avg", "sum", "std_dev"}, reducer.required) {
		is_numerical_column = true
	}

	// avoid calculating if not necessary; to save space
	is_required_std_dev := false
	if utility.ExistsIn("std_dev", reducer.required) {
		is_required_std_dev = true
	}

	for {
		select {
		case data := <-channel:
			var value float64
			var err error
			if is_numerical_column {
				value, err = strconv.ParseFloat(data, 64)
				if err != nil && data != "" {
					is_numerical_column = false
					// return nil, fmt.Errorf("value provided is not numeric: %s", data)
				}

				if value > reducer.stats["max"] {
					reducer.stats["max"] = value
				}
				if value < reducer.stats["min"] {
					reducer.stats["min"] = value
				}
				reducer.stats["sum"] += value

				if is_required_std_dev {
					reducer.values = append(reducer.values, value)
				}
			}

			if data == "" {
				reducer.stats["nulls"]++
			} else {
				reducer.stats["count"]++
			}

		case <-waitCh:
			close(channel)
			reducer.stats["mean"] = reducer.stats["sum"] / reducer.stats["count"]

			// avoid calculating if not necessary to save space
			if is_required_std_dev {
				var variance float64
				for _, value := range reducer.values {
					variance += math.Pow(reducer.stats["mean"]-value, 2)
				}
				reducer.stats["std_dev"] = math.Sqrt(variance / reducer.stats["sum"])
			}
			if !is_numerical_column {
				return map[string]float64{"nulls": reducer.stats["nulls"], "count": reducer.stats["count"]}
			} else {
				return reducer.stats
			}
		}
	}
}

func (reducer *Reducer) ReduceColumns(channel chan string, wg *sync.WaitGroup) {
	waitCh := make(chan int)
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(waitCh)
	}(wg)

	for {
		select {
		case <-waitCh:
			return
		case line := <-channel:
			if reducer.limit > 0 {
				reducer.out.Write([]byte(line + "\n"))
				reducer.limit--
			} else if reducer.limit == -1 {
				reducer.out.Write([]byte(line + "\n"))
			} else {
				return
			}
		}
	}
}
