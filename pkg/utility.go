package csvutil

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func CopyToTemp(file io.Reader) *os.File {
	fd, _ := os.CreateTemp("", "stdin_temp")
	data, _ := io.ReadAll(file)
	fd.Write(data)
	return fd
}

func ExistsIn(value string, list []string) bool {
	for _, val := range list {
		if val == value {
			return true
		}
	}
	return false
}

func mapHeaders(filename string) map[string]int {
	fd, _ := os.Open(filename)
	defer fd.Close()
	reader := bufio.NewReader(fd)
	mapped_headers := make(map[string]int)
	headers_line, _ := reader.ReadBytes('\n')
	headers := strings.Split(strings.Trim(string(headers_line), "\n"), ",")
	for index, value := range headers {
		mapped_headers[value] = index
	}
	return mapped_headers
}

func adjustLimit(filename string, offset int64, chunk_size int64) (limit int64) {
	fd, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("can't open file %s", filename))
	}
	defer fd.Close()

	fd.Seek(offset+chunk_size, io.SeekStart)
	reader := bufio.NewReader(fd)
	line, _ := reader.ReadBytes('\n')
	limit = chunk_size + int64(len(line))
	return limit
}

func statFile(filename string) int64 {
	if filename == "" {
		return 0
	} else {
		stat, _ := os.Stat(filename)
		return stat.Size()
	}
}
