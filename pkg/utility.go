package csvutil

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func ConstructStringFromList(list []string) string {
	var result string = "{"
	for _, value := range list {
		result += value
		if value != list[len(list)-1] {
			result += ","
		}
	}
	result += "}"
	return result
}

func CopyToTemp(file *os.File) *os.File {
	fd, _ := os.CreateTemp("", "stdin_temp")
	data, _ := io.ReadAll(os.Stdin)
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
	headers := strings.Split(string(headers_line), ",")
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