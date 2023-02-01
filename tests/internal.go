package csvutil

import (
	"strings"
	"testing"
)

var filename = "input.csv"

func expect(t *testing.T, actual_error error, expected_error error) {
	if actual_error == nil {
		t.Errorf("command succeeded when it should have failed")
	} else if actual_error.Error() != expected_error.Error() {
		t.Errorf("expected error \"%s\", got \"%s\"", expected_error.Error(), actual_error.Error())
	}
}

func cleanOutput(input []byte) string {
	return strings.Trim(strings.ReplaceAll(string(input), " ", ""), "\n")
}
