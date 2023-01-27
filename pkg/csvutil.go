package csvutil

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
)

var wg sync.WaitGroup

type Options struct {
	Filename    string
	Threads     int
	Mode        string
	Filters     map[string]string
	Group       string
	Delimiter   string
	Columns     []string
	KeepHeaders bool
	Limit       int
	Output      *os.File
	Stats       []string
}

func setupMappers(filename string, thread_count int, delimiter string) (mappers []*Mapper) {
	threads := int64(thread_count)
	file_size := statFile(filename)

	var offset int64 = 0
	var chuck_size int64 = file_size / threads
	var limit int64 = 0

	if filename == os.Stdin.Name() {
		mappers = append(mappers, newMapper(0, offset, math.MaxInt64, filename, delimiter))
	} else {
		for i := int64(0); i < threads; i++ {
			limit = adjustLimit(filename, offset, chuck_size)
			mappers = append(mappers, newMapper(i, offset, limit, filename, delimiter))
			offset += limit
		}
	}

	return mappers
}

func setupFilters(filename string, mappers []*Mapper, filters map[string]string) error {
	mapped_headers := mapHeaders(filename)
	mapper_headers := make(map[int]string)
	for key, value := range filters {
		if _, ok := mapped_headers[key]; !ok {
			return fmt.Errorf("filter: column %s doesn't exist", key)
		}
		mapper_headers[mapped_headers[key]] = value
	}
	for _, mapper := range mappers {
		mapper.setColumns(mapper_headers)
	}
	return nil
}

func Count(option *Options) (map[string]int64, error) {
	mapped_headers := mapHeaders(option.Filename)
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

	mappers := setupMappers(option.Filename, option.Threads, option.Delimiter)

	for _, mapper := range mappers {
		mapper.setMode(option.Mode)
		mapper.setSkipHeaders(true)
		if option.Group != "" {
			mapper.group = mapped_headers[option.Group]
		}
	}

	if len(option.Filters) != 0 {
		err := setupFilters(option.Filename, mappers, option.Filters)
		if err != nil {
			return nil, err
		}
	}

	// run mappers
	for _, mapper := range mappers {
		wg.Add(1)
		go mapper.runCount()
	}

	wg.Wait()
	return newReducer().reduceCount(mappers[:], option.Mode), nil
}

func Stat(option *Options) (map[string]float64, error) {
	mapped_headers := mapHeaders(option.Filename)
	_, exists := mapped_headers[option.Columns[0]]
	if !exists {
		return map[string]float64{}, fmt.Errorf("stat: column %s doesn't exist", option.Columns[0])
	}

	mappers := setupMappers(option.Filename, option.Threads, option.Delimiter)
	channel := make(chan string)

	for _, mapper := range mappers {
		mapper.setChannel(channel)
		mapper.setSkipHeaders(true)
	}

	mapper_headers := make(map[int]string)
	for col_name, col_index := range mapped_headers {
		if col_name == option.Columns[0] {
			mapper_headers[col_index] = "0"
		}
	}

	for _, mapper := range mappers {
		mapper.setColumns(mapper_headers)
		wg.Add(1)
		go mapper.runStat()
	}

	return newStatReducer(option.Stats).reduceStat(channel)
}

func Columns(option *Options) error {
	var ordering []int
	headers := mapHeaders(option.Filename)
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

	mappers := setupMappers(option.Filename, option.Threads, option.Delimiter)
	channel := make(chan string)

	for _, mapper := range mappers {
		mapper.setChannel(channel)
	}

	if !option.KeepHeaders || option.Output != os.Stdout {
		for _, mapper := range mappers {
			mapper.setSkipHeaders(true)
		}
	}

	if len(option.Filters) != 0 {
		setupFilters(option.Filename, mappers, option.Filters)
	}

	for _, mapper := range mappers {
		mapper.setOrdering(ordering)
		wg.Add(1)
		go mapper.runColumns()
	}

	newColumnsReducer(option.Output, option.Limit).reduceColumns(channel)
	return nil
}
