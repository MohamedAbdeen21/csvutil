package csvutil

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCount(t *testing.T) {
	r, w, _ := os.Pipe()
	expected := "total:2845343"
	cmd := RootCmd()
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
	cmd := RootCmd()
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

func TestCountWithNonExistantantFilter(t *testing.T) {
	column := "name"
	expected := fmt.Errorf("filter: column %s doesn't exist", column)
	cmd := RootCmd()
	cmd.SetArgs([]string{"count", filename, "-t", "12", "-f", fmt.Sprintf("%s=NonExistant", column)})
	cmd.SetErr(io.Discard)
	expect(t, cmd.Execute(), expected)
}

func TestCountMissingColumn(t *testing.T) {
	cmd := RootCmd()
	column := "some_column"
	expected_error := fmt.Errorf("group: column %s doesn't exist", column)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"count", filename, "-g", column, "-t", "6"})
	expect(t, cmd.Execute(), expected_error)
}

func TestCountGroup(t *testing.T) {
	r, w, _ := os.Pipe()
	expected := []string{"US/Eastern:1221927", "US/Pacific:967094", "NULL:3659", "US/Central:488065", "US/Mountain:164597"}
	sort.Strings(expected)
	cmd := RootCmd()
	cmd.SetArgs([]string{"count", filename, "-t", "6", "-g", "Timezone"})
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
	expected := fmt.Errorf("mode must be one of the possible values %v", CountPossibleModes)
	cmd := RootCmd()
	cmd.SetArgs([]string{"count", filename, "-m", "words"})
	cmd.SetErr(io.Discard)
	expect(t, cmd.Execute(), expected)
}

func TestCountStdin(t *testing.T) {
	read, write, _ := os.Pipe()
	r, w, _ := os.Pipe()
	expected := "total:20"

	cmd1 := exec.Command("head", filename, "-n", "20")
	cmd1.Stdout = write
	err := cmd1.Run()
	if err != nil {
		t.Error(err.Error())
	}
	write.Close()

	cmd2 := RootCmd()
	cmd2.SetIn(read)
	cmd2.SetOut(w)
	cmd2.SetArgs([]string{"count"})
	cmd2.Execute()
	w.Close()

	output, _ := io.ReadAll(r)
	actual := cleanOutput(output)
	if actual != expected {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestCountGroupStdin(t *testing.T) {
	read, write, _ := os.Pipe()
	r, w, _ := os.Pipe()
	expected := []string{"IN:12", "KY:1", "MI:2", "OH:31", "PA:2", "WV:1"}
	sort.Strings(expected)

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
	cmd2.SetArgs([]string{"count", "-g", "State"})
	cmd2.Execute()
	w.Close()

	output, _ := io.ReadAll(r)
	actual := strings.Split(cleanOutput(output), "\n")
	sort.Strings(actual)
	if !cmp.Equal(actual, expected) {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}
