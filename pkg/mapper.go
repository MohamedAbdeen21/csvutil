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

	// select columns
	ordering    []int
	skipHeaders bool
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

func (mapper *Mapper) setOrdering(ordering []int) {
	mapper.ordering = ordering
}

func (mapper *Mapper) setSkipHeaders(val bool) {
	mapper.skipHeaders = val
}

func (mapper *Mapper) _skipHeader(reader *bufio.Reader) {
	if mapper.id == 0 {
		line, _ := reader.ReadBytes('\n')
		mapper.bytes_count += int64(len(line))
		mapper.lines_count += 1
	}
}

func (mapper *Mapper) _readLine(reader *bufio.Reader) (line []byte, ok bool, eof bool) {
	line, err := reader.ReadBytes('\n')
	mapper.bytes_count += int64(len(line))
	if err == io.EOF {
		return line, false, true
	}

	if mapper.bytes_count == mapper.limit {
		return line, false, false
	}

	if mapper.bytes_count > mapper.limit {
		panic("Reader read more than it should")
	}

	return line, true, false
}

func (mapper *Mapper) _filter(line string) bool {
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

func (mapper *Mapper) _countLines(line string) {
	if mapper._filter(line) {
		mapper.lines_count++
	} else {
		return
	}
}

func (mapper *Mapper) _countGroups(line string) {
	row := strings.Split(line, mapper.delimiter)
	for index, value := range row {
		if index != mapper.group {
			continue
		}
		if value == "" {
			value = "NULL"
		}
		if mapper._filter(line) {
			mapper.group_count[value]++
		}
	}
}

func (mapper *Mapper) count(line string) {
	switch mapper.mode {
	case "lines", "bytes":
		mapper._countLines(line)
	case "group":
		mapper._countGroups(line)
	}
}

func (mapper *Mapper) selectColumns(line string) (string, bool) {
	if mapper._filter(line) {
		row := strings.Split(line, mapper.delimiter)
		var new_line = make([]string, len(mapper.ordering))
		for pos, index := range mapper.ordering {
			new_line[pos] = row[index]
		}
		return strings.Join(new_line, mapper.delimiter), true
	} else {
		return "", false
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
	return ""
}

func (mapper *Mapper) runCount() {
	mapper.file.Seek(mapper.offset, io.SeekStart)
	defer mapper.file.Close()
	defer wg.Done()
	reader := bufio.NewReader(mapper.file)

	if mapper.skipHeaders {
		mapper._skipHeader(reader)
	}

	for {
		line, ok, eof := mapper._readLine(reader)
		if eof {
			break
		}

		mapper.count(string(line))

		if !ok {
			break
		}

	}
}

func (mapper *Mapper) runStat() {
	mapper.file.Seek(mapper.offset, io.SeekStart)
	defer mapper.file.Close()
	defer wg.Done()
	reader := bufio.NewReader(mapper.file)

	if mapper.skipHeaders {
		mapper._skipHeader(reader)
	}

	for {
		line, ok, eof := mapper._readLine(reader)
		if eof {
			break
		}

		mapper.channel <- mapper.stat(string(line))
		if !ok {
			break
		}

	}
}

func (mapper *Mapper) runColumns() {
	mapper.file.Seek(mapper.offset, io.SeekStart)
	defer mapper.file.Close()
	defer wg.Done()
	reader := bufio.NewReader(mapper.file)

	if mapper.skipHeaders {
		mapper._skipHeader(reader)
	}

	for {
		line, ok, eof := mapper._readLine(reader)

		if eof {
			break
		}

		data, isValid := mapper.selectColumns(string(line))
		if isValid {
			mapper.channel <- data
		}

		if !ok {
			break
		}
	}
}
