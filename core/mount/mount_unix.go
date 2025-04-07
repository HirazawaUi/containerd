//go:build !windows && !openbsd

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package mount

import (
	"fmt"
	"github.com/containerd/log"
	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"sort"
	"time"
)

// UnmountRecursive unmounts the target and all mounts underneath, starting
// with the deepest mount first.
func UnmountRecursive(target string, flags int) error {
	defer func() {
		if r := recover(); r != nil {
			// 将 panic 转换为 error 并记录日志
			err := fmt.Errorf("panic occurred while unmounting %s: %v", target, r)
			log.L.Errorf("recovered from panic: %v", err)
		}
	}()

	if target == "" {
		return nil
	}

	target, err := CanonicalizePath(target)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}

	mounts, err := mountinfo.GetMounts(mountinfo.PrefixFilter(target))
	if err != nil {
		return err
	}

	targetSet := make(map[string]struct{})
	for _, m := range mounts {
		targetSet[m.Mountpoint] = struct{}{}
	}

	var targets []string
	for m := range targetSet {
		targets = append(targets, m)
	}

	// Make the deepest mount be first
	sort.SliceStable(targets, func(i, j int) bool {
		return len(targets[i]) > len(targets[j])
	})

	for _, target := range targets {
		log.L.Infof("find unmounting target: %s", target)
		if err := UnmountAll(target, flags); err != nil {
			log.L.Errorf("failed to unmount '%s' (child of '%s'): %w, retry.....", target, targets[len(targets)-1], err)
			if err := killProcessesBindingPath(target); err != nil {
				return fmt.Errorf("failed to kill target process: %w", err)
			}
			if err := UnmountAll(target, flags); err != nil {
				return fmt.Errorf("failed to unmount target: %w", err)
			}
			return nil
		}
	}
	return nil
}

// killProcessesBindingPath 查找并杀死绑定到指定路径的所有进程
func killProcessesBindingPath(path string) error {
	cmd := exec.Command("fuser", "-k", "-m", path)
	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("failed to kill processes binding to %s: %s, %w", path, string(output), err)
	}

	time.Sleep(100 * time.Millisecond)
	return nil
}

func unmount(target string, flags int) error {
	if isFUSE(target) {
		// TODO: Why error is ignored?
		// Shouldn't this just be unconditional "return unmountFUSE(target)"?
		if err := unmountFUSE(target); err == nil {
			return nil
		}
	}
	for i := 0; i < 50; i++ {
		if err := unix.Unmount(target, flags); err != nil {
			switch err {
			case unix.EBUSY:
				time.Sleep(50 * time.Millisecond)
				continue
			default:
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("failed to unmount target %s: %w", target, unix.EBUSY)
}

// Unmount the provided mount path with the flags
func Unmount(target string, flags int) error {
	if err := unmount(target, flags); err != nil && err != unix.EINVAL {
		return err
	}
	return nil
}

// UnmountAll repeatedly unmounts the given mount point until there
// are no mounts remaining (EINVAL is returned by mount), which is
// useful for undoing a stack of mounts on the same mount point.
// UnmountAll all is noop when the first argument is an empty string.
// This is done when the containerd client did not specify any rootfs
// mounts (e.g. because the rootfs is managed outside containerd)
// UnmountAll is noop when the mount path does not exist.
func UnmountAll(mount string, flags int) error {
	if mount == "" {
		return nil
	}
	if _, err := os.Stat(mount); os.IsNotExist(err) {
		return nil
	}

	for {
		if err := unmount(mount, flags); err != nil {
			// EINVAL is returned if the target is not a
			// mount point, indicating that we are
			// done. It can also indicate a few other
			// things (such as invalid flags) which we
			// unfortunately end up squelching here too.
			if err == unix.EINVAL {
				return nil
			}
			return err
		}
	}
}
