package csvutil

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
)

var wg sync.WaitGroup

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

func Count(filename string, instances int, mode string, filters map[string]string, group string, delimiter string) (map[string]int64, error) {

	mappers := setupMappers(filename, instances, delimiter)

	for _, mapper := range mappers {
		mapper.setMode(mode)
		mapper.setSkipHeaders(true)
	}

	if group != "" {
		valid := false
		group = strings.TrimSpace(group)
		mapped_headers := mapHeaders(filename)
		for key := range mapped_headers {
			if key == group {
				valid = true
				break
			}
		}

		if !valid {
			return nil, fmt.Errorf("group: column %s doesn't exist", group)
		}

		for _, mapper := range mappers {
			mapper.group = mapped_headers[group]
		}
	}

	if len(filters) != 0 {
		err := setupFilters(filename, mappers, filters)
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
	return newReducer().reduceCount(mappers[:], mode), nil
}

func Stat(filename string, column string, instances int, stats []string, delimiter string) (map[string]float64, error) {
	mappers := setupMappers(filename, instances, delimiter)
	channel := make(chan string)

	for _, mapper := range mappers {
		mapper.setChannel(channel)
		mapper.setSkipHeaders(true)
	}

	mapped_headers := mapHeaders(filename)
	mapper_headers := make(map[int]string)
	for col_name, col_index := range mapped_headers {
		if col_name == column {
			mapper_headers[col_index] = "0"
		}
	}

	for _, mapper := range mappers {
		mapper.setColumns(mapper_headers)
		wg.Add(1)
		go mapper.runStat()
	}

	return newStatReducer(stats).reduceStat(channel)
}

func Columns(filename string, columns []string, filters map[string]string, instances int, keepHeaders bool, limit int, delimiter string, output *os.File) error {
	mappers := setupMappers(filename, instances, delimiter)
	channel := make(chan string)

	for _, mapper := range mappers {
		mapper.setChannel(channel)
	}

	if !keepHeaders || output != os.Stdout {
		for _, mapper := range mappers {
			mapper.setSkipHeaders(true)
		}
	}

	if len(filters) != 0 {
		setupFilters(filename, mappers, filters)
	}

	var ordering []int
	headers := mapHeaders(filename)
	if len(columns) == 0 {
		// all columns in the correct order
		ordering = make([]int, len(headers))
		for _, index := range headers {
			ordering[index] = index
		}
	} else {
		ordering = make([]int, len(columns))
		for pos, col_name := range columns {
			index, exists := headers[col_name]
			if exists {
				ordering[pos] = index
			} else {
				return fmt.Errorf("select: column %s doesn't exist", col_name)
			}
		}
	}

	for _, mapper := range mappers {
		mapper.setOrdering(ordering)
		wg.Add(1)
		go mapper.runColumns()
	}

	newColumnsReducer(output, limit).reduceColumns(channel)
	return nil
}
