package csvutil

import (
	"io"
	"os"
	"testing"
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
