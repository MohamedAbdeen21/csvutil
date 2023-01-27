package csvutil_test

import (
	"os/exec"
	"strings"
	"testing"

	internal "github.com/MohamedAbdeen21/csvutil/testing/internal"
)

func TestCount(t *testing.T) {
	expected := "total: 1154730978"
	msg, err := internal.RunCmd("./csvutil count input.csv --threads=12 --mode=bytes")
	if err != nil {
		t.Error(msg)
	} else {
		if msg != expected {
			t.Error("output of count is not correct")
		}
	}
}

func TestCountGroup(t *testing.T) {
	expected := map[string]bool{"3: 155105": true, "1: 26053": true, "2: 2532991": true, "4: 131193": true}
	msg, err := internal.RunCmd("./csvutil count input.csv -g Severity")
	if err != nil {
		t.Error(msg)
	} else {
		output := strings.Split(strings.Trim(msg, "\n"), "\n")
		for _, line := range output {
			if _, exists := expected[line]; !exists {
				t.Error("output of count group is not correct")
			}
		}
	}
}

func TestCountGroupWithFilter(t *testing.T) {
	expected := map[string]bool{"4: 5222": true, "1: 502": true, "3: 1936": true, "2: 16749": true}
	msg, err := internal.RunCmd("./csvutil count input.csv -g Severity -f State=OH")
	if err != nil {
		t.Error(msg)
	} else {
		output := strings.Split(msg, "\n")
		for _, line := range output {
			if _, exists := expected[line]; !exists {
				t.Error("output of count group with filter is not correct")
			}
		}
	}
}

func TestCountGroupWithNonExistantColumn(t *testing.T) {
	msg, err := internal.RunCmd("./csvutil count input.csv -g nonExistant -f State=OH")
	if err == nil {
		t.Error("should fail on Non-Existant columns")
	} else if err != err.(*exec.ExitError) {
		t.Error(msg)
	}
}
