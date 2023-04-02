package util

import (
	"errors"
	"os/exec"
	"strconv"
	"strings"
)

func KillProcessByName(name string, signal string) error {
	pid, err := getProcessIDByName(name)
	if err != nil {
		return err
	}

	cmd := exec.Command("kill", "-"+signal, strconv.Itoa(pid))
	return cmd.Run()
}

func getProcessIDByName(name string) (int, error) {
	cmd := exec.Command("ps", "-C", name, "-o", "pid=")
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	if len(output) == 0 {
		return 0, errors.New("process not found")
	}

	pidStr := strings.TrimSpace(string(output))
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return 0, err
	}

	return pid, nil
}
