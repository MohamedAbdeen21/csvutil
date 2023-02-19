package mapper

import (
	"bufio"
	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
	"io"
	"strings"
)

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
			if isFound && !utility.ExistsIn(value, match) {
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
		if value == mapper.nulls {
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

func (mapper *Mapper) stat(line string) string {
	row := strings.Split(line, mapper.delimiter)
	for index := range mapper.columns {
		if row[index] == mapper.nulls {
			return ""
		} else {
			return row[index]
		}
	}
	return ""
}
