package mapper

import (
	"bufio"
	"io"
	"os"
	"strings"
	"sync"
)

type Mapper struct {
	// common
	id        int64
	offset    int64
	limit     int64
	file      *os.File
	delimiter string
	columns   map[int][]string
	nulls     string

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

func NewMapper(id int64, offset int64, limit int64, filename string, delimiter string, nulls string) *Mapper {
	fd, _ := os.Open(filename)
	return &Mapper{
		id:          id,
		offset:      offset,
		limit:       limit,
		file:        fd,
		delimiter:   delimiter,
		nulls:       nulls,
		group_count: make(map[string]int64),
	}
}

func (mapper *Mapper) SetGroup(column_index int) *Mapper {
	mapper.group = column_index
	return mapper
}

func (mapper *Mapper) SetColumns(columns map[int][]string) *Mapper {
	mapper.columns = columns
	return mapper
}

func (mapper *Mapper) SetMode(mode string) *Mapper {
	mapper.mode = mode
	return mapper
}

func (mapper *Mapper) SetChannel(channel chan string) *Mapper {
	mapper.channel = channel
	return mapper
}

func (mapper *Mapper) SetOrdering(ordering []int) *Mapper {
	mapper.ordering = ordering
	return mapper
}

func (mapper *Mapper) SetSkipHeaders(val bool) *Mapper {
	mapper.skipHeaders = val
	return mapper
}

func (mapper *Mapper) GetLinesCount() int64 {
	return mapper.lines_count
}

func (mapper *Mapper) GetGroupCount() map[string]int64 {
	return mapper.group_count
}

func (mapper *Mapper) GetBytesCount() int64 {
	return mapper.bytes_count
}

func (mapper *Mapper) RunCount(wg *sync.WaitGroup) {
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

func (mapper *Mapper) RunStat(wg *sync.WaitGroup) {
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

func (mapper *Mapper) RunColumns(wg *sync.WaitGroup) {
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
		data = strings.Trim(data, "\n")
		if isValid {
			mapper.channel <- data
		}

		if !ok {
			break
		}
	}
}
