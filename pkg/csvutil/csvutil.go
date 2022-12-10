package csvutil

import (
	"fmt"
	"math"
	"os"
	"sync"
)

var wg sync.WaitGroup

func setupMappers(filename string, thread_count int, delimiter string) (mappers []*Mapper) {
	threads := int64(thread_count)
	file_size := statFile(filename)

	var offset int64 = 0
	var chuck_size int64 = file_size / threads
	var limit int64 = 0

	if filename == "" {
		mappers = append(mappers, newMapper(0, 0, int64(math.MaxInt64), os.Stdin.Name(), delimiter))
	} else {
		for i := int64(0); i < threads; i++ {
			limit = adjustLimit(filename, offset, chuck_size)
			mappers = append(mappers, newMapper(i, offset, limit, filename, delimiter))
			offset += limit
		}
	}
	return mappers
}

func Count(filename string, instances int, mode string, filters map[string]string, group string, delimiter string) map[string]int64 {

	mappers := setupMappers(filename, instances, delimiter)

	for _, mapper := range mappers {
		mapper.setMode(mode)
	}

	if group != "" {
		for _, mapper := range mappers {
			mapper.group = mapHeaders(filename)[group]
		}
	}

	if len(filters) != 0 {
		mapped_headers := mapHeaders(filename)
		mapper_headers := make(map[int]string)
		for key, value := range filters {
			if _, ok := mapped_headers[key]; !ok {
				panic(fmt.Sprintf("Column %s doesn't exist", key))
			}
			mapper_headers[mapped_headers[key]] = value
		}
		for _, mapper := range mappers {
			mapper.setColumns(mapper_headers)
		}
	}

	// run mappers
	for _, mapper := range mappers {
		wg.Add(1)
		go mapper.runCount()
	}

	wg.Wait()
	return newReducer().reduceCount(mappers[:], mode)
}

func Stat(filename string, instances int, delimiter, column string, stats []string) map[string]float64 {
	mappers := setupMappers(filename, instances, delimiter)
	channel := make(chan string)

	for _, mapper := range mappers {
		mapper.setChannel(channel)
	}

	mapped_headers := mapHeaders(filename)
	mapper_headers := make(map[int]string)
	for col_name, col_index := range mapped_headers {
		if col_name == column {
			mapper_headers[col_index] = "0"
			for _, mapper := range mappers {
				mapper.setColumns(mapper_headers)
			}
		}
	}

	for _, mapper := range mappers {
		wg.Add(1)
		go mapper.runStat()
	}

	return newReducer(stats).reduceStat(channel)
}
