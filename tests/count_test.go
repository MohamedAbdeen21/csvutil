package csvutil

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	csvutil "github.com/MohamedAbdeen21/csvutil/cmd"
	"github.com/google/go-cmp/cmp"
)

func TestCount(t *testing.T) {
	r, w, _ := os.Pipe()
	expected := "total:2845343"
	cmd := csvutil.RootCmd()
	cmd.SetArgs([]string{"count", filename, "-t", "6"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := cleanOutput(result)
	if actual != expected {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestCountWithFilter(t *testing.T) {
	r, w, _ := os.Pipe()
	expected := "total:32555"
	cmd := csvutil.RootCmd()
	cmd.SetArgs([]string{"count", filename, "-t", "6", "-f", "State=WA"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := cleanOutput(result)
	if actual != expected {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestCountWithNonExistantFilter(t *testing.T) {
	column := "name"
	expected := fmt.Errorf("filter: column %s doesn't exist", column)
	cmd := csvutil.RootCmd()
	cmd.SetArgs([]string{"count", filename, "-t", "12", "-f", fmt.Sprintf("%s=NonExistant", column)})
	cmd.SetErr(io.Discard)
	expect(t, cmd.Execute(), expected)
}

func TestCountMissingColumn(t *testing.T) {
	cmd := csvutil.RootCmd()
	column := "some_column"
	expected_error := fmt.Errorf("group: column %s doesn't exist", column)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"count", filename, "-g", column, "-t", "6"})
	expect(t, cmd.Execute(), expected_error)
}

func TestCountGroup(t *testing.T) {
	r, w, _ := os.Pipe()
	expected := []string{"4:131193", "1:26053", "3:155105", "2:2532991"}
	sort.Strings(expected)
	cmd := csvutil.RootCmd()
	cmd.SetArgs([]string{"count", filename, "-t", "6", "-g", "Severity"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := strings.Split(cleanOutput(result), "\n")
	sort.Strings(actual)
	if !cmp.Equal(actual, expected) {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestInvalidCountMode(t *testing.T) {
	expected := fmt.Errorf("mode must be one of the possible values %v", csvutil.CountPossibleModes)
	cmd := csvutil.RootCmd()
	cmd.SetArgs([]string{"count", filename, "-m", "words"})
	cmd.SetErr(io.Discard)
	expect(t, cmd.Execute(), expected)
}

// TODO: implement a test for reading from stdin
func TestCountStdin(t *testing.T) {}
