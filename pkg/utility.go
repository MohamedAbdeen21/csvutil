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
	mapped_headers := make(map[string]int)
	reader := bufio.NewReader(fd)
	headers_line, _ := reader.ReadString('\n')
	headers := strings.Split(headers_line, ",")
	for index, value := range headers {
		mapped_headers[value] = index
	}
	return mapped_headers
}

func adjustLimit(filename string, offset int64, chuck_size int64) (limit int64) {
	fd, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("can't open file: %s", filename))
	}
	defer fd.Close()
	if err != nil {
		panic("No such file!")
	}

	fd.Seek(offset+chuck_size, io.SeekStart)
	reader := bufio.NewReader(fd)
	line, _ := reader.ReadBytes('\n')
	limit = chuck_size + int64(len(line))
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
