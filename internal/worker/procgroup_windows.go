//go:build windows

package worker

import (
	"os/exec"
	"strconv"
)

// configureProcessGroup configures cmd to kill the whole process tree on cancellation (timeout).
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// taskkill /F /T /PID terminates the process and any child processes it spawned.
		_ = exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
		return nil
	}
}
