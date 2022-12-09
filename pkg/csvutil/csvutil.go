package csvutil

import (
	"fmt"
	"math"
	"os"
)

func setupMappers(filename string, thread_count int, delimiter string) (mappers []*Mapper) {
	threads := int64(thread_count)
	file_size, fd := openAndStatFile(filename)

	var offset int64 = 0
	var chuck_size int64 = file_size / threads
	var limit int64 = 0

	if fd == os.Stdin {
		mappers = append(mappers, newMapper(0, 0, int64(math.MaxInt64), fd, delimiter))
	} else {
		for i := int64(0); i < threads; i++ {
			limit = adjustLimit(filename, offset, chuck_size)
			mappers = append(mappers, newMapper(i, offset, limit, fd, delimiter))
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
			mapper.group = mapHeaders(mapper.file)[group]
		}
	}

	if len(filters) != 0 {
		mapped_headers := mapHeaders(mappers[0].file)
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
		mapper.runCount()
	}

	return countReducer(mappers[:], mode)
}

func Stat(filename string, instances int, delimiter, column string) {
	mappers := setupMappers(filename, instances, delimiter)
	var channels []chan string

	for _, mapper := range mappers {
		channel := make(chan string)
		channels = append(channels, channel)
		mapper.setChannel(channel)
	}

	statReducer(channels)
}
