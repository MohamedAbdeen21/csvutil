package csvutil_test_internal

import (
	"os/exec"
	"strings"
)

func RunCmd(command string) (string, error) {
	args := strings.Split(command, " ")
	cmd := exec.Command(args[0], args[1:]...)
	msg, err := cmd.CombinedOutput()
	return strings.Trim(string(msg), "\n"), err
}
