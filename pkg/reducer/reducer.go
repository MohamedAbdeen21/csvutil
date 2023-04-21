package reducer

import (
	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
	"io"
	"math"
	"strconv"
	"sync"
)

type Reducer struct {
	limit   int
	channel chan map[string]string

	// stat
	required []string
	values   []float64
	stats    map[string]float64

	// channel
	out io.Writer
}

func NewReducer(channel chan map[string]string) *Reducer {
	return &Reducer{
		channel: channel,
	}
}

func NewStatReducer(channel chan map[string]string, required_stats []string) *Reducer {
	reducer := &Reducer{
		stats:    make(map[string]float64),
		required: required_stats,
		channel:  channel,
	}
	reducer.stats["min"] = math.MaxInt64
	reducer.stats["max"] = math.MinInt64
	reducer.stats["nulls"] = 0
	return reducer
}

func NewColumnsReducer(channel chan map[string]string, fd io.Writer, limit int) *Reducer {
	return &Reducer{
		out:     fd,
		limit:   limit,
		channel: channel,
	}
}

func (reducer *Reducer) ReduceCount(mode string, wg *sync.WaitGroup) map[string]int {
	wg.Wait()
	close(reducer.channel)
	var result map[string]int = make(map[string]int)
	for {
		data, more := <-reducer.channel
		if !more {
			break
		}

		switch mode {
		case "lines":
			value, _ := strconv.Atoi(data["lines"])
			result["total"] += value
		case "bytes":
			value, _ := strconv.Atoi(data["bytes"])
			result["total"] += value
		case "group":
			for key, value := range data {
				v, _ := strconv.Atoi(value)
				result[key] += v
			}
		}
	}
	return result
}

func (reducer *Reducer) ReduceStat(wg *sync.WaitGroup) map[string]float64 {
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
		case data := <-reducer.channel:
			var value float64
			var err error
			if is_numerical_column {
				value, err = strconv.ParseFloat(data[""], 64)
				if err != nil && data[""] != "" {
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

			if data[""] == "" {
				reducer.stats["nulls"]++
			} else {
				reducer.stats["count"]++
			}

		case <-waitCh:
			close(reducer.channel)
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

func (reducer *Reducer) ReduceColumns(channel chan map[string]string, wg *sync.WaitGroup) {
	waitCh := make(chan int)
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(waitCh)
	}(wg)

	for {
		select {
		case <-waitCh:
			return
		case data := <-channel:
			line := data[""]
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
