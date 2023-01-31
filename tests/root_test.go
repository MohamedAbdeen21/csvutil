package csvutil_test

import (
	"fmt"
	"io"
	"testing"

	cmd "github.com/MohamedAbdeen21/csvutil/cmd"
)

func TestThreadsFlag(t *testing.T) {
	cmd := cmd.RootCmd()
	cmd.SetArgs([]string{"-t", "12"})
	cmd.Execute()
}

func TestInvalidThreadFlag(t *testing.T) {
	cmd := cmd.RootCmd()
	expected_error := fmt.Errorf("threads can't be less than 1")
	cmd.SetArgs([]string{"-t", "-1"})
	cmd.SetErr(io.Discard)
	expect(t, cmd, expected_error)
}

func TestDelimiterFlag(t *testing.T) {
	cmd := cmd.RootCmd()
	cmd.SetArgs([]string{"-t", "2", "-d", "\t"})
	cmd.Execute()
}

func TestInvalidDelimiterFlag(t *testing.T) {
	cmd := cmd.RootCmd()
	expected_error := fmt.Errorf("delimiter must be a single character")
	cmd.SetArgs([]string{"-d", "ab", "-t", "2"})
	cmd.SetErr(io.Discard)
	expect(t, cmd, expected_error)
}
