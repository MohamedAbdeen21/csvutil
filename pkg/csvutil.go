package csvutil

import (
	"fmt"
	"github.com/MohamedAbdeen21/csvutil/pkg/mapper"
	"github.com/MohamedAbdeen21/csvutil/pkg/reducer"
	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
	"io"
	"strings"
	"sync"
)

type Options struct {
	Filename    string
	Threads     int
	Mode        string
	Filters     map[string][]string
	Group       string
	Delimiter   string
	Columns     []string
	KeepHeaders bool
	Limit       int
	Output      io.Writer
	Stats       []string
	Nulls       string
}

func setupMappers(filename string, thread_count int, delimiter string, nulls string) (mappers []*mapper.Mapper) {
	threads := int64(thread_count)
	file_size := utility.StatFile(filename)

	var offset int64 = 0
	var chunk_size int64 = file_size / threads
	var limit int64 = 0

	for i := int64(0); i < threads; i++ {
		limit = utility.AdjustLimit(filename, offset, chunk_size)
		mappers = append(mappers, mapper.NewMapper(i, offset, limit, filename, delimiter, nulls))
		offset += limit
	}

	return mappers
}

func setupFilters(filename string, mappers []*mapper.Mapper, filters map[string][]string) error {
	mapped_headers := utility.MapHeaders(filename)
	mapper_headers := make(map[int][]string)
	for key, values := range filters {
		if _, ok := mapped_headers[key]; !ok {
			return fmt.Errorf("filter: column %s doesn't exist", key)
		}
		mapper_headers[mapped_headers[key]] = values
	}
	for _, mapper := range mappers {
		mapper.SetColumns(mapper_headers)
	}
	return nil
}

func Count(option *Options) (map[string]int64, error) {
	var wg = sync.WaitGroup{}
	mapped_headers := utility.MapHeaders(option.Filename)
	if option.Group != "" {
		valid := false
		option.Group = strings.TrimSpace(option.Group)
		for key := range mapped_headers {
			if key == option.Group {
				valid = true
				break
			}
		}

		if !valid {
			return nil, fmt.Errorf("group: column %s doesn't exist", option.Group)
		}

	}

	mappers := setupMappers(option.Filename, option.Threads, option.Delimiter, option.Nulls)

	for _, mapper := range mappers {
		mapper.SetMode(option.Mode)
		mapper.SetSkipHeaders(true)
		if option.Group != "" {
			mapper.SetGroup(mapped_headers[option.Group])
		}
	}

	if len(option.Filters) != 0 {
		err := setupFilters(option.Filename, mappers, option.Filters)
		if err != nil {
			return nil, err
		}
	}

	for _, mapper := range mappers {
		wg.Add(1)
		go mapper.RunCount(&wg)
	}

	return reducer.NewReducer().ReduceCount(mappers[:], option.Mode, &wg), nil
}

func Stat(option *Options) (map[string]float64, error) {
	wg := sync.WaitGroup{}
	mapped_headers := utility.MapHeaders(option.Filename)
	_, exists := mapped_headers[option.Columns[0]]
	if !exists {
		return map[string]float64{}, fmt.Errorf("stat: column %s doesn't exist", option.Columns[0])
	}

	mappers := setupMappers(option.Filename, option.Threads, option.Delimiter, option.Nulls)
	channel := make(chan string)

	for _, mapper := range mappers {
		mapper.SetChannel(channel)
		mapper.SetSkipHeaders(true)
	}

	mapper_headers := make(map[int][]string)
	for col_name, col_index := range mapped_headers {
		if col_name == option.Columns[0] {
			mapper_headers[col_index] = []string{"0"}
		}
	}

	for _, mapper := range mappers {
		mapper.SetColumns(mapper_headers)
		wg.Add(1)
		go mapper.RunStat(&wg)
	}

	return reducer.NewStatReducer(option.Stats).ReduceStat(channel, &wg), nil
}

func Columns(option *Options) error {
	wg := sync.WaitGroup{}
	var ordering []int
	headers := utility.MapHeaders(option.Filename)
	if len(option.Columns) == 0 {
		// all columns in the correct order
		ordering = make([]int, len(headers))
		for _, index := range headers {
			ordering[index] = index
		}
	} else {
		ordering = make([]int, len(option.Columns))
		for pos, col_name := range option.Columns {
			index, exists := headers[col_name]
			if exists {
				ordering[pos] = index
			} else {
				return fmt.Errorf("select: column %s doesn't exist", col_name)
			}
		}
	}

	mappers := setupMappers(option.Filename, option.Threads, option.Delimiter, option.Nulls)
	channel := make(chan string)

	for _, mapper := range mappers {
		mapper.SetChannel(channel)
	}

	if !option.KeepHeaders {
		for _, mapper := range mappers {
			mapper.SetSkipHeaders(true)
		}
	}

	if len(option.Filters) != 0 {
		setupFilters(option.Filename, mappers, option.Filters)
	}

	for _, mapper := range mappers {
		mapper.SetOrdering(ordering)
		wg.Add(1)
		go mapper.RunColumns(&wg)
	}

	reducer.NewColumnsReducer(option.Output, option.Limit).ReduceColumns(channel, &wg)
	return nil
}
