package csvutil_test

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

type void struct{}

var filename = "input.csv"

func expect(t *testing.T, root *cobra.Command, expected_error error) {
	if err := root.Execute(); err == nil {
		t.Errorf("command succeeded when it should have failed")
	} else if err.Error() != expected_error.Error() {
		t.Errorf("expected error \"%s\", got \"%s\"", expected_error.Error(), err.Error())
	}
}

func cleanOutput(input []byte) string {
	return strings.Trim(strings.ReplaceAll(string(input), " ", ""), "\n")
}
