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

func TestStat(t *testing.T) {
	r, w, _ := os.Pipe()
	expected := []string{"mean:61.79", "max:196.00", "nulls:69274"}
	sort.Strings(expected)
	cmd := RootCmd()
	cmd.SetArgs([]string{"stat", filename, "-c", "Temperature(F)", "-t", "12", "-s", "mean,max,nulls"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := strings.Split(cleanOutput(result), "\n")
	sort.Strings(actual)
	if !cmp.Equal(expected, actual) {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}

func TestStatMissingColumn(t *testing.T) {
	cmd := RootCmd()
	column := "some_column"
	expected_error := fmt.Errorf("stat: column %s doesn't exist", column)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"stat", filename, "-c", column})
	expect(t, cmd.Execute(), expected_error)
}

func TestStatStdin(t *testing.T) {
	read, write, _ := os.Pipe()
	r, w, _ := os.Pipe()
	expected := []string{"std_dev:0.28", "min:2.00", "max:3.00", "nulls:0", "count:9", "sum:20.00", "mean:2.22"}
	sort.Strings(expected)

	cmd1 := exec.Command("head", filename, "-n", "10")
	cmd1.Stdout = write
	err := cmd1.Run()
	if err != nil {
		t.Error(err.Error())
	}
	write.Close()

	cmd2 := RootCmd()
	cmd2.SetIn(read)
	cmd2.SetOut(w)
	cmd2.SetArgs([]string{"stat", "-c", "Severity"})
	cmd2.Execute()
	w.Close()

	output, _ := io.ReadAll(r)
	actual := strings.Split(cleanOutput(output), "\n")
	sort.Strings(actual)
	if !cmp.Equal(actual, expected) {
		t.Errorf("expected %v, found %v", expected, actual)
	}
}
