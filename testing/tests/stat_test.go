package csvutil_test

import (
	"os/exec"
	"strings"
	"testing"

	internal "github.com/MohamedAbdeen21/csvutil/testing/internal"
)

func TestStat(t *testing.T) {
	expected := map[string]bool{"mean:61.79": true, "min:-89.00": true, "max:196.00": true}
	msg, err := internal.RunCmd("./csvutil stat input.csv Temperature(F) --stat=mean,max,min --threads=12")
	if err != nil {
		t.Error(string(msg))
	} else {
		output := strings.Split(strings.ReplaceAll(msg, " ", ""), "\n")
		for _, line := range output {
			if _, exists := expected[line]; !exists {
				t.Error("output of stat is not correct")
			}
		}
	}
}

func TestStatOnNonNumericalColumn(t *testing.T) {
	msg, err := internal.RunCmd("./csvutil stat input.csv Country --threads=12")
	if err == nil {
		t.Error("should fail on Non-Numerical columns")
	} else if err != err.(*exec.ExitError) {
		t.Error(string(msg))
	}
}

func TestStatOnNonExistantColumn(t *testing.T) {
	msg, err := internal.RunCmd("./csvutil stat input.csv other --threads=12")
	if err == nil {
		t.Error("should fail on Non-Existant columns")
	} else if err != err.(*exec.ExitError) {
		t.Error(msg)
	}
}
