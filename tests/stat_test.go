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

func TestStat(t *testing.T) {
	r, w, _ := os.Pipe()
	expected := []string{"mean:61.79", "max:196.00", "nulls:69274"}
	sort.Strings(expected)
	cmd := csvutil.RootCmd()
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
	cmd := csvutil.RootCmd()
	column := "some_column"
	expected_error := fmt.Errorf("stat: column %s doesn't exist", column)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"stat", filename, "-c", column})
	expect(t, cmd.Execute(), expected_error)
}

// TODO: implement a test for reading from stdin
func TestStatStdin(t *testing.T) {}
