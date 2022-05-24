package cgroups

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"

	"github.com/shuveb/containers-the-hard-way/utils"
)

type Accessor struct {
}

func GetAccessor() Accessor {
	return Accessor{}
}

func (c Accessor) CreateCGroups(containerID string, createCGroupDirs bool) {
	cgroups := []string{"/sys/fs/cgroup/memory/fdocker/" + containerID,
		"/sys/fs/cgroup/pids/fdocker/" + containerID,
		"/sys/fs/cgroup/cpu/fdocker/" + containerID,
		"/sys/fs/cgroup/blkio/fdocker/" + containerID,
		"/sys/fs/cgroup/devices/fdocker/" + containerID,
	}

	if createCGroupDirs {
		utils.MustWithMsg(utils.EnsureDirs(cgroups),
			"Unable to create cgroup directories")
	}

	for _, cgroupDir := range cgroups {
		utils.MustWithMsg(ioutil.WriteFile(cgroupDir+"/notify_on_release", []byte("1"), 0700),
			"Unable to write to cgroup notification file")
		utils.MustWithMsg(ioutil.WriteFile(cgroupDir+"/cgroup.procs",
			[]byte(strconv.Itoa(os.Getpid())), 0700), "Unable to write to cgroup procs file")
	}
}

func (c Accessor) RemoveCGroups(containerID string) {
	cgroups := []string{"/sys/fs/cgroup/memory/fdocker/" + containerID,
		"/sys/fs/cgroup/pids/fdocker/" + containerID,
		"/sys/fs/cgroup/cpu/fdocker/" + containerID,
		"/sys/fs/cgroup/blkio/fdocker/" + containerID,
		"/sys/fs/cgroup/devices/fdocker/" + containerID,
	}

	for _, cgroupDir := range cgroups {
		utils.MustWithMsg(os.Remove(cgroupDir), "Unable to remove cgroup dir")
	}
}

func (c Accessor) setMemoryLimit(containerID string, limitMB int, swapLimitInMB int) {
	memFilePath := "/sys/fs/cgroup/memory/fdocker/" + containerID +
		"/memory.limit_in_bytes"
	swapFilePath := "/sys/fs/cgroup/memory/fdocker/" + containerID +
		"/memory.memsw.limit_in_bytes"
	utils.MustWithMsg(ioutil.WriteFile(memFilePath,
		[]byte(strconv.Itoa(limitMB*1024*1024)), 0644),
		"Unable to write memory limit")

	/*
		memory.memsw.limit_in_bytes contains the total amount of memory the
		control group can consume: this includes both swap and RAM.
		If if memory.limit_in_bytes is specified but memory.memsw.limit_in_bytes
		is left untouched, processes in the control group will continue to
		consume swap space.
	*/
	if swapLimitInMB >= 0 {
		utils.MustWithMsg(ioutil.WriteFile(swapFilePath,
			[]byte(strconv.Itoa((limitMB*1024*1024)+(swapLimitInMB*1024*1024))),
			0644), "Unable to write memory limit")
	}
}

func (c Accessor) setCpuLimit(containerID string, limit float64) {
	cfsPeriodPath := "/sys/fs/cgroup/cpu/fdocker/" + containerID +
		"/cpu.cfs_period_us"
	cfsQuotaPath := "/sys/fs/cgroup/cpu/fdocker/" + containerID +
		"/cpu.cfs_quota_us"

	if limit > float64(runtime.NumCPU()) {
		fmt.Printf("Ignoring attempt to set CPU quota to great than number of available CPUs")
		return
	}

	utils.MustWithMsg(ioutil.WriteFile(cfsPeriodPath,
		[]byte(strconv.Itoa(1000000)), 0644),
		"Unable to write CFS period")

	utils.MustWithMsg(ioutil.WriteFile(cfsQuotaPath,
		[]byte(strconv.Itoa(int(1000000*limit))), 0644),
		"Unable to write CFS period")

}

func (c Accessor) setPidsLimit(containerID string, limit int) {
	maxProcsPath := "/sys/fs/cgroup/pids/fdocker/" + containerID +
		"/pids.max"

	utils.MustWithMsg(ioutil.WriteFile(maxProcsPath,
		[]byte(strconv.Itoa(limit)), 0644),
		"Unable to write pids limit")

}

func (c Accessor) setReadbps(containerID string, limit string) {
	fmt.Println("read_bps_device: " + limit)
	path := "/sys/fs/cgroup/blkio/fdocker/" + containerID +
		"/blkio.throttle.read_bps_device"
	utils.MustWithMsg(ioutil.WriteFile(path,
		[]byte(limit), 0644),
		"Unable to setReadbps")
}
func (c Accessor) setReadiops(containerID string, limit string) {
	fmt.Println("read_iops_device: " + limit)
	path := "/sys/fs/cgroup/blkio/fdocker/" + containerID +
		"/blkio.throttle.read_iops_device"
	utils.MustWithMsg(ioutil.WriteFile(path,
		[]byte(limit), 0644),
		"Unable to setReadiops")
}
func (c Accessor) setWritebps(containerID string, limit string) {
	fmt.Println("write_bps_device: " + limit)
	path := "/sys/fs/cgroup/blkio/fdocker/" + containerID +
		"/blkio.throttle.write_bps_device"
	utils.MustWithMsg(ioutil.WriteFile(path,
		[]byte(limit), 0644),
		"Unable to setWritebps")
}
func (c Accessor) setWriteiops(containerID string, limit string) {
	fmt.Println("write_iops_device: " + limit)
	path := "/sys/fs/cgroup/blkio/fdocker/" + containerID +
		"/blkio.throttle.write_iops_device"
	utils.MustWithMsg(ioutil.WriteFile(path,
		[]byte(limit), 0644),
		"Unable to setWriteiops")
}
func (c Accessor) ConfigureCGroups(containerID string, mem int, swap int, pids int, cpus float64, read_bps string, read_iops string, write_bps string, write_iops string) {
	if mem > 0 {
		c.setMemoryLimit(containerID, mem, swap)
	}
	if cpus > 0 {
		c.setCpuLimit(containerID, cpus)
	}
	if pids > 0 {
		c.setPidsLimit(containerID, pids)
	}
	if read_bps != "" {
		c.setReadbps(containerID, read_bps)
	}
	if read_iops != "" {
		c.setReadiops(containerID, read_iops)
	}
	if write_bps != "" {
		c.setWritebps(containerID, write_bps)
	}
	if write_iops != "" {
		c.setWriteiops(containerID, write_iops)
	}
}
