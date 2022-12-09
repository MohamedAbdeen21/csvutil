package csvutil

import (
	"bufio"
	"io"
	"os"
	"strings"
)

func mapHeaders(fd *os.File) map[string]int {
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
	file, err := os.Open(filename)
	if err != nil {
		panic("No such file!")
	}

	file.Seek(offset+chuck_size, io.SeekStart)
	reader := bufio.NewReader(file)
	line, _ := reader.ReadBytes('\n')
	limit = chuck_size + int64(len(line))
	return limit
}

func openAndStatFile(filename string) (int64, *os.File) {
	if filename == "" {
		return 0, os.Stdin
	} else {
		file, _ := os.Open(filename)
		stat, _ := os.Stat(filename)
		return stat.Size(), file
	}
}
