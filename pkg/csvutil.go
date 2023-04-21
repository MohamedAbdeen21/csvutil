package csvutil

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/MohamedAbdeen21/csvutil/pkg/mapper"
	"github.com/MohamedAbdeen21/csvutil/pkg/reducer"
	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
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

func setupMappers(
	filename string,
	thread_count int,
	delimiter string,
	nulls string,
) (mappers []*mapper.Mapper, err error) {
	threads := int64(thread_count)
	file_size := utility.StatFile(filename)

	var offset int64 = 0
	var chunk_size int64 = file_size / threads
	var limit int64 = 0

	for i := int64(0); i < threads; i++ {
		limit, err = utility.AdjustLimit(filename, offset, chunk_size)
		if err != nil {
			return nil, err
		}
		mappers = append(mappers, mapper.NewMapper(i, offset, limit, filename, delimiter, nulls))
		offset += limit
	}

	return mappers, nil
}

func setupFilters(filename string, filters map[string][]string) (map[int][]string, error) {
	mapped_headers := utility.MapHeaders(filename)
	mapper_headers := make(map[int][]string)

	for key, values := range filters {
		if _, exists := mapped_headers[key]; !exists {
			return nil, fmt.Errorf("filter: column %s doesn't exist", key)
		}
		mapper_headers[mapped_headers[key]] = values
	}

	return mapper_headers, nil
}

func Count(option *Options) (map[string]int64, error) {
	wg := sync.WaitGroup{}

	mapped_headers := utility.MapHeaders(option.Filename)
	if option.Group != "" {
		option.Group = strings.TrimSpace(option.Group)
		if _, exists := mapped_headers[option.Group]; !exists {
			return nil, fmt.Errorf("group: column %s doesn't exist", option.Group)
		}
	}

	mappers, err := setupMappers(option.Filename, option.Threads, option.Delimiter, option.Nulls)
	if err != nil {
		return nil, err
	}

	for _, mapper := range mappers {
		if option.Group != "" {
			mapper.SetGroup(mapped_headers[option.Group])
		}
	}

	var mapper_headers map[int][]string
	if len(option.Filters) != 0 {
		mapper_headers, err = setupFilters(option.Filename, option.Filters)
		if err != nil {
			return nil, err
		}
	}

	for _, mapper := range mappers {
		wg.Add(1)
		go mapper.
			SetColumns(mapper_headers).
			SetMode(option.Mode).
			SetSkipHeaders(true).
			RunCount(&wg)
	}

	return reducer.NewReducer().ReduceCount(mappers, option.Mode, &wg), nil
}

func Stat(option *Options) (map[string]float64, error) {
	wg := sync.WaitGroup{}
	mapped_headers := utility.MapHeaders(option.Filename)
	_, exists := mapped_headers[option.Columns[0]]
	if !exists {
		return map[string]float64{}, fmt.Errorf("stat: column %s doesn't exist", option.Columns[0])
	}

	mappers, err := setupMappers(option.Filename, option.Threads, option.Delimiter, option.Nulls)
	if err != nil {
		return nil, err
	}

	channel := make(chan string)

	mapper_headers := make(map[int][]string)
	for col_name, col_index := range mapped_headers {
		if col_name == option.Columns[0] {
			mapper_headers[col_index] = []string{"0"}
		}
	}

	for _, mapper := range mappers {
		wg.Add(1)
		go mapper.
			SetColumns(mapper_headers).
			SetChannel(channel).
			SetSkipHeaders(true).
			RunStat(&wg)
	}

	return reducer.NewStatReducer(option.Stats).ReduceStat(channel, &wg), nil
}

func Columns(option *Options) error {
	wg := sync.WaitGroup{}

	var ordering []int
	headers := utility.MapHeaders(option.Filename)
	if len(option.Columns) == 0 {
		// all columns and in the correct order
		ordering = make([]int, len(headers))
		for _, index := range headers {
			ordering[index] = index
		}
	} else {
		ordering = make([]int, len(option.Columns))
		for pos, col_name := range option.Columns {
			if index, exists := headers[col_name]; exists {
				ordering[pos] = index
			} else {
				return fmt.Errorf("select: column %s doesn't exist", col_name)
			}
		}
	}

	mappers, err := setupMappers(option.Filename, option.Threads, option.Delimiter, option.Nulls)
	if err != nil {
		return err
	}

	channel := make(chan string)

	var mapper_headers map[int][]string
	if len(option.Filters) != 0 {
		// Non-existant columns will be caught above
		mapper_headers, _ = setupFilters(option.Filename, option.Filters)
	}

	for _, mapper := range mappers {
		wg.Add(1)
		go mapper.
			SetColumns(mapper_headers).
			SetSkipHeaders(!option.KeepHeaders).
			SetOrdering(ordering).
			SetChannel(channel).
			RunColumns(&wg)
	}

	reducer.NewColumnsReducer(option.Output, option.Limit).ReduceColumns(channel, &wg)
	return nil
}
