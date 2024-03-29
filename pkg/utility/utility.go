package utility

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
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

func ListExsistsIn(list1, list2 []string) bool {
	set := make(map[string]bool)
	for _, value := range list1 {
		set[value] = true
	}
	for _, value := range list2 {
		if set[value] {
			return true
		}
	}
	return false
}

func MapHeaders(filename string) map[string]int {
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

func AdjustLimit(filename string, offset int64, chunk_size int64) (limit int64, err error) {
	fd, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("unexpected error, can't open file %s", filename)
	}
	defer fd.Close()

	fd.Seek(offset+chunk_size, io.SeekStart)
	reader := bufio.NewReader(fd)
	line, _ := reader.ReadBytes('\n')
	limit = chunk_size + int64(len(line))
	return limit, nil
}

func StatFile(filename string) int64 {
	if filename == "" {
		return 0
	} else {
		stat, _ := os.Stat(filename)
		return stat.Size()
	}
}

func OpenBrowser(filename string) error {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", filename).Run()
	case "windows":
		err = exec.Command("cmd", "/c", "start", filename).Run()
	case "darwin":
		err = exec.Command("open", filename).Run()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	return err
}
