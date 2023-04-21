package plotter

import (
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

type tuple struct {
	key   string
	value int64
}

func sortCountData(data map[string]int64) []tuple {
	size := len(data)
	buffer := make([]tuple, size)
	var index int
	for key, value := range data {
		buffer[index].key = key
		buffer[index].value = value
		index++
	}

	sort.Slice(buffer, func(x, y int) bool {
		x_float, err1 := strconv.ParseFloat(buffer[x].key, 64)
		y_float, err2 := strconv.ParseFloat(buffer[y].key, 64)
		if err1 == nil && err2 == nil {
			return x_float < y_float
		}

		x_int, err1 := strconv.ParseInt(buffer[x].key, 10, 64)
		y_int, err2 := strconv.ParseInt(buffer[y].key, 10, 64)
		if err1 == nil && err2 == nil {
			return x_int < y_int
		}

		return buffer[x].key < buffer[y].key
	})
	return buffer
}

func processDataBar(data map[string]int64) ([]string, []opts.BarData) {
	buffer := sortCountData(data)
	size := len(buffer)
	keys := make([]string, size)
	var values []opts.BarData
	for index, v := range buffer {
		keys[index] = v.key
		values = append(values, opts.BarData{Value: v.value})
	}
	return keys, values
}

func BarPlotGroup(column string, data map[string]int64, outputFile string) error {
	keys, values := processDataBar(data)
	bar := charts.NewBar()
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: fmt.Sprintf("Bar plot of %s column", column)}),
		charts.WithTooltipOpts(opts.Tooltip{
			TriggerOn: "mousemove",
			Show:      true,
		}))

	bar.SetXAxis(keys).
		AddSeries("Frequency", values)

	f, _ := os.Create(outputFile)
	defer f.Close()
	bar.Render(f)
	err := utility.OpenBrowser(outputFile)
	if err != nil {
		return fmt.Errorf("can't open browser %v", err)
	}
	return nil
}

func processDataScatter(data map[string]int64) ([]string, []opts.ScatterData) {
	buffer := sortCountData(data)
	size := len(buffer)
	keys := make([]string, size)
	var values []opts.ScatterData
	for index, v := range buffer {
		keys[index] = v.key
		values = append(values, opts.ScatterData{Value: v.value})
	}
	return keys, values
}

func ScatterPlotGroup(column string, data map[string]int64, outputFile string) error {
	keys, values := processDataScatter(data)
	scatter := charts.NewScatter()
	scatter.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: fmt.Sprintf("Scatter plot of %s column", column)}),
		charts.WithTooltipOpts(opts.Tooltip{
			TriggerOn: "mousemove",
			Show:      true,
		}))
	scatter.SetXAxis(keys).AddSeries("Frequency", values)

	f, _ := os.Create(outputFile)
	defer f.Close()
	scatter.Render(f)
	err := utility.OpenBrowser(outputFile)
	if err != nil {
		return fmt.Errorf("can't open browser %v", err)
	}
	return nil
}

func processDataLine(data map[string]int64) ([]string, []opts.LineData) {
	buffer := sortCountData(data)
	size := len(buffer)
	keys := make([]string, size)
	var values []opts.LineData
	for index, v := range buffer {
		keys[index] = v.key
		values = append(values, opts.LineData{Value: v.value})
	}
	return keys, values
}

func LinePlotGroup(column string, data map[string]int64, outputFile string) error {
	keys, values := processDataLine(data)
	line := charts.NewLine()
	line.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: fmt.Sprintf("Scatter plot of %s column", column)}),
		charts.WithTooltipOpts(opts.Tooltip{
			TriggerOn: "mousemove",
			Show:      true,
		}))
	line.SetXAxis(keys).AddSeries("Frequency", values)

	f, _ := os.Create(outputFile)
	defer f.Close()
	line.Render(f)
	err := utility.OpenBrowser(outputFile)
	if err != nil {
		return fmt.Errorf("can't open browser %v", err)
	}
	return nil
}
