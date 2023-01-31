package csvutil_test

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	cmd "github.com/MohamedAbdeen21/csvutil/cmd"
)

func TestStat(t *testing.T) {
	r, w, _ := os.Pipe()
	var null void
	set := make(map[string]void)
	expected := []string{"mean:61.79", "max:196.00", "nulls:69274"}
	for _, key := range expected {
		set[key] = null
	}
	cmd := cmd.RootCmd()
	cmd.SetArgs([]string{"stat", filename, "-c", "Temperature(F)", "-t", "12", "-s", "mean,max,nulls"})
	cmd.SetOut(w)
	cmd.Execute()
	w.Close()
	result, _ := io.ReadAll(r)
	actual := strings.Split(cleanOutput(result), "\n")
	for _, line := range actual {
		if _, exists := set[line]; exists == false {
			t.Errorf("expected %v, found %v", expected, actual)
			t.FailNow()
		}
	}
}

func TestNonExistantFile(t *testing.T) {
	cmd := cmd.RootCmd()
	non_existant_file := "nonExistant.csv"
	expected_error := fmt.Errorf("file %s doesn't exist", non_existant_file)
	cmd.SetArgs([]string{"stat", non_existant_file})
	cmd.SetErr(io.Discard)
	expect(t, cmd, expected_error)
}

func TestMissingColumn(t *testing.T) {
	cmd := cmd.RootCmd()
	column := "some_column"
	expected_error := fmt.Errorf("stat: column %s doesn't exist", column)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"stat", filename, "-c", column})
	expect(t, cmd, expected_error)
}
