package csvutil

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSelectWithLimit(t *testing.T) {
	fd, _ := os.Open("testdata/Select10.txt")
	data, _ := io.ReadAll(fd)
	expected := string(data)
	r, w, _ := os.Pipe()
	cmd := RootCmd()
	cmd.SetArgs([]string{"select", filename, "-n", "10"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := string(result)
	if actual != expected {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestSelectWithColumns(t *testing.T) {
	fd, _ := os.Open("testdata/Select10ID.txt")
	data, _ := io.ReadAll(fd)
	expected := string(data)
	r, w, _ := os.Pipe()
	cmd := RootCmd()
	cmd.SetArgs([]string{"select", filename, "-n", "10", "-c", "ID"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := string(result)
	if actual != expected {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestSelectWithFilters(t *testing.T) {
	fd, _ := os.Open("testdata/Select30Filter.txt")
	data, _ := io.ReadAll(fd)
	expected := string(data)
	r, w, _ := os.Pipe()
	cmd := RootCmd()
	cmd.SetArgs([]string{"select", filename, "-n", "30", "-f", "Severity=2"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := string(result)
	if actual != expected {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestSelectStdin(t *testing.T) {
	read, write, _ := os.Pipe()
	r, w, _ := os.Pipe()
	fd, _ := os.Open("testdata/SelectStdin50.txt")
	data, _ := io.ReadAll(fd)
	expected := strings.Split(cleanOutput(data), "\n")

	cmd1 := exec.Command("head", filename, "-n", "50")
	cmd1.Stdout = write
	err := cmd1.Run()
	if err != nil {
		t.Error(err.Error())
	}
	write.Close()

	cmd2 := RootCmd()
	cmd2.SetIn(read)
	cmd2.SetOut(w)
	cmd2.SetArgs([]string{"select", "-c", "State"})
	cmd2.Execute()
	w.Close()

	output, _ := io.ReadAll(r)
	actual := strings.Split(cleanOutput(output), "\n")
	if !cmp.Equal(actual, expected) {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestSelectWithMissingColumn(t *testing.T) {
	cmd := RootCmd()
	column := "some_column"
	expected_error := fmt.Errorf("select: column %s doesn't exist", column)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"select", filename, "-c", column})
	expect(t, cmd.Execute(), expected_error)
}

func TestSelectWithSkipHeaders(t *testing.T) {
	r, w, _ := os.Pipe()

	cmd1 := exec.Command("head", filename, "-n", "11")
	result, _ := cmd1.Output()
	expected := strings.Split(cleanOutput(result), "\n")[1:] // remove header line

	cmd2 := RootCmd()
	cmd2.SetOut(w)
	cmd2.SetArgs([]string{"select", filename, "--headers=false", "-n", "10"})
	cmd2.Execute()
	w.Close()

	output, _ := io.ReadAll(r)
	actual := strings.Split(cleanOutput(output), "\n")
	if !cmp.Equal(actual, expected) {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}
