package csvutil

import (
	"bufio"
	"io"
	"os"
	"strings"
)

type Mapper struct {
	// common
	id        int64
	offset    int64
	limit     int64
	file      *os.File
	delimiter string
	columns   map[int]string

	// stat
	channel chan string

	// count
	mode        string
	group       int
	lines_count int64
	bytes_count int64
	group_count map[string]int64
}

func newMapper(id int64, offset int64, limit int64, filename string, delimiter string) *Mapper {
	fd, _ := os.Open(filename)
	return &Mapper{
		id:          id,
		offset:      offset,
		limit:       limit,
		file:        fd,
		delimiter:   delimiter,
		group_count: make(map[string]int64),
	}
}

func (mapper *Mapper) setColumns(columns map[int]string) {
	mapper.columns = columns
}

func (mapper *Mapper) setMode(mode string) {
	mapper.mode = mode
}

func (mapper *Mapper) setChannel(channel chan string) {
	mapper.channel = channel
}

func (mapper *Mapper) _filterCount(line string) bool {
	if len(mapper.columns) == 0 {
		return true
	} else {
		values := strings.Split(line, mapper.delimiter)
		for index, value := range values {
			match, isFound := mapper.columns[index]
			if isFound && value != match {
				return false
			}
		}
	}
	return true
}

func (mapper *Mapper) _count_lines(line string) {
	if mapper._filterCount(line) {
		mapper.lines_count++
	} else {
		return
	}
}

func (mapper *Mapper) _count_groups(line string) {
	row := strings.Split(line, mapper.delimiter)
	for index, value := range row {
		if index != mapper.group {
			continue
		}
		if value == "" {
			value = "NULL"
		}
		mapper.group_count[value]++
	}
}

func (mapper *Mapper) count(line string) {
	switch mapper.mode {
	case "lines", "bytes":
		mapper._count_lines(line)
	case "group":
		mapper._count_groups(line)
	}
}

func (mapper *Mapper) getCount() map[string]int64 {
	switch mapper.mode {
	case "lines":
		return map[string]int64{"total": mapper.lines_count}
	case "bytes":
		return map[string]int64{"total": mapper.bytes_count}
	case "group":
		return mapper.group_count
	default:
		return map[string]int64{"total": mapper.lines_count}
	}
}

func (mapper *Mapper) stat(line string) string {
	row := strings.Split(line, mapper.delimiter)
	for index := range mapper.columns {
		return row[index]
	}
	panic("Column not found!")
}

func (mapper *Mapper) runCount() {
	mapper.file.Seek(mapper.offset, io.SeekStart)
	defer mapper.file.Close()
	reader := bufio.NewReader(mapper.file)

	// skip headers
	if mapper.id == 0 && mapper.mode == "group" {
		line, _ := reader.ReadBytes('\n')
		mapper.bytes_count += int64(len(line))
		mapper.lines_count += 1
	}

	for {
		line, err := reader.ReadBytes('\n')
		mapper.bytes_count += int64(len(line))
		if err == io.EOF {
			break
		}

		mapper.count(string(line))

		if mapper.bytes_count == mapper.limit {
			break
		}

		if mapper.bytes_count > mapper.limit {
			panic("Reader read more than it should")
		}
	}
	wg.Done()
}

func (mapper *Mapper) runStat() {
	mapper.file.Seek(mapper.offset, io.SeekStart)
	defer mapper.file.Close()
	reader := bufio.NewReader(mapper.file)

	if mapper.id == 0 {
		line, _ := reader.ReadBytes('\n')
		mapper.bytes_count += int64(len(line))
		mapper.lines_count += 1
	}

	for {
		line, err := reader.ReadBytes('\n')
		mapper.bytes_count += int64(len(line))
		if err == io.EOF {
			break
		}

		mapper.channel <- mapper.stat(string(line))

		if mapper.bytes_count == mapper.limit {
			break
		}

		if mapper.bytes_count > mapper.limit {
			panic("Reader read more than it should")
		}
	}
	wg.Done()
}
