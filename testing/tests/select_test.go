package csvutil_test

import (
	"os/exec"
	"strings"
	"testing"

	internal "github.com/MohamedAbdeen21/csvutil/testing/internal"
)

func TestSelect(t *testing.T) {
	_, err := internal.RunCmd("./csvutil select input.csv -c Country")
	if err != nil {
		t.Error("an error has occurred while running")
	}
}

func TestSelectWithNoHeaders(t *testing.T) {
	expected := "A-1,3,2016-02-08 00:37:08,2016-02-08 06:37:08,40.108909999999995,-83.09286,40.11206,-83.03187,3.23,Between Sawmill Rd/Exit 20 and OH-315/Olentangy Riv Rd/Exit 22 - Accident.,,Outerbelt E,R,Dublin,Franklin,OH,43017,US,US/Eastern,KOSU,2016-02-08 00:53:00,42.1,36.1,58.0,29.76,10.0,SW,10.4,0.0,Light Rain,False,False,False,False,False,False,False,False,False,False,False,False,False,Night,Night,Night,Night"
	msg, err := internal.RunCmd("./csvutil select input.csv --headers=false --limit=1")
	if err != nil {
		t.Error("an error has occurred while running")
	} else if msg != expected {
		t.Error("output of select with skip headers is not correct")
	}
}

func TestSelectWithLimit(t *testing.T) {
	expected := 20
	msg, err := internal.RunCmd("./csvutil select input.csv -c ID --headers=false --limit=20 --threads=12")
	if err != nil {
		t.Error("an error has occurred while running")
	} else if len(strings.Split(msg, "\n")) != expected {
		t.Error("output of select with limit is not correct")
	}
}

func TestSelectWithFilters(t *testing.T) {
	expected := "A-1"
	msg, err := internal.RunCmd("./csvutil select input.csv -c ID -f ID=A-1")
	if err != nil {
		t.Error("an error has occurred while running")
	} else if msg != expected {
		t.Error("output of select with filter is not correct")
	}
}

func TestSelectNonExistantColumn(t *testing.T) {
	msg, err := internal.RunCmd("./csvutil select input.csv -c nonExistant -f ID=A-1")
	if err == nil {
		t.Error("should fail on Non-Existant columns")
	} else if err != err.(*exec.ExitError) {
		t.Error(msg)
	}
}

func TestSelectNonExistantFile(t *testing.T) {
	msg, err := internal.RunCmd("./csvutil select random.csv -c ID")
	if err == nil {
		t.Error("should fail on Non-Existant columns")
	} else if err != err.(*exec.ExitError) {
		t.Error(msg)
	}
}
