package csvutil

import (
	"fmt"
	"io"
	"testing"
)

func TestThreadsFlag(t *testing.T) {
	cmd := RootCmd()
	cmd.SetArgs([]string{"-t", "12"})
	cmd.Execute()
}

func TestInvalidThreadFlag(t *testing.T) {
	cmd := RootCmd()
	expected_error := fmt.Errorf("threads can't be less than 1")
	cmd.SetArgs([]string{"-t", "-1"})
	cmd.SetErr(io.Discard)
	expect(t, cmd.Execute(), expected_error)
}

func TestDelimiterFlag(t *testing.T) {
	cmd := RootCmd()
	cmd.SetArgs([]string{"-t", "2", "-d", "\t"})
	cmd.Execute()
}

func TestInvalidDelimiterFlag(t *testing.T) {
	cmd := RootCmd()
	expected_error := fmt.Errorf("delimiter must be a single character")
	cmd.SetArgs([]string{"-d", "ab", "-t", "2"})
	cmd.SetErr(io.Discard)
	expect(t, cmd.Execute(), expected_error)
}

func TestNonExistantFile(t *testing.T) {
	cmd := RootCmd()
	non_existant_file := "nonExistant.csv"
	expected_error := fmt.Errorf("file %s doesn't exist", non_existant_file)
	cmd.SetArgs([]string{"stat", non_existant_file})
	cmd.SetErr(io.Discard)
	expect(t, cmd.Execute(), expected_error)
}
