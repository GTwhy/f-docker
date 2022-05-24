package childmode

import (
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/shuveb/containers-the-hard-way/cgroups"
	"github.com/shuveb/containers-the-hard-way/image"
	"github.com/shuveb/containers-the-hard-way/network"
	"github.com/shuveb/containers-the-hard-way/utils"
	"github.com/shuveb/containers-the-hard-way/workdirs"
	flag "github.com/spf13/pflag"
	"golang.org/x/sys/unix"
)

type Executor struct {
}

func New() Executor {
	return Executor{}
}

func (e Executor) CmdName() string {
	return "child-mode"
}

func (e Executor) Implicit() bool {
	return true
}

func (e Executor) Usage() string {
	return ""
}

func (e Executor) Exec() {
	fs := flag.FlagSet{}
	fs.ParseErrorsWhitelist.UnknownFlags = true

	mem := fs.Int("mem", -1, "Max RAM to allow in  MB")
	swap := fs.Int("swap", -1, "Max swap to allow in  MB")
	pids := fs.Int("pids", -1, "Number of max processes to allow")
	cpus := fs.Float64("cpus", -1, "Number of CPU cores to restrict to")
	read_bps := fs.String("device-read-bps", "", "Max rate to read from dev")
	read_iops := fs.String("device-read-iops", "", "Max times to read from dev in one second")
	write_bps := fs.String("device-write-bps", "", "Number of max rate to write into the dev")
	write_iops := fs.String("device-write-iops", "", "Max times to write into the dev in one second")
	image := fs.String("img", "", "Container image")
	if err := fs.Parse(os.Args[2:]); err != nil {
		fmt.Println("Error parsing: ", err)
	}
	if len(fs.Args()) < 2 {
		log.Fatalf("Please pass image name and command to run")
	}
	execContainerCommand(*mem, *swap, *pids, *cpus, *read_bps, *read_iops, *write_bps, *write_iops, fs.Args()[0], *image, fs.Args()[1:])
}

/*
	Called if this program is executed with "child-mode" as the first argument
*/
func execContainerCommand(mem int, swap int, pids int, cpus float64, read_bps string, read_iops string, write_bps string, write_iops string,
	containerID string, imageShaHex string, args []string) {
	mntPath := workdirs.GetContainerFSHome(containerID) + "/mnt"
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	imgAccessor := image.GetAccessor()
	netAccessor := network.GetAccessor()
	cGroupsAccessor := cgroups.GetAccessor()
	imgConfig := imgAccessor.ParseContainerConfig(imageShaHex)
	utils.MustWithMsg(unix.Sethostname([]byte(containerID)), "Unable to set hostname")
	utils.MustWithMsg(netAccessor.JoinContainerNetworkNamespace(containerID), "Unable to join container network namespace")
	cGroupsAccessor.CreateCGroups(containerID, true)
	cGroupsAccessor.ConfigureCGroups(containerID, mem, swap, pids, cpus, read_bps, read_iops, write_bps, write_iops)
	utils.MustWithMsg(copyNameserverConfig(containerID), "Unable to copy resolve.conf")
	utils.MustWithMsg(unix.Chroot(mntPath), "Unable to chroot")
	utils.MustWithMsg(os.Chdir("/"), "Unable to change directory")
	utils.Must(utils.EnsureDirs([]string{"/proc", "/sys"}))
	utils.MustWithMsg(unix.Mount("proc", "/proc", "proc", 0, ""), "Unable to mount proc")
	utils.MustWithMsg(unix.Mount("tmpfs", "/tmp", "tmpfs", 0, ""), "Unable to mount tmpfs")
	utils.MustWithMsg(unix.Mount("tmpfs", "/dev", "tmpfs", 0, ""), "Unable to mount tmpfs on /dev")
	utils.Must(utils.EnsureDirs([]string{"/dev/pts"}))
	utils.MustWithMsg(unix.Mount("devpts", "/dev/pts", "devpts", 0, ""), "Unable to mount devpts")
	utils.MustWithMsg(unix.Mount("sysfs", "/sys", "sysfs", 0, ""), "Unable to mount sysfs")
	netAccessor.SetupLocalInterface()
	cmd.Env = imgConfig.Config.Env
	if err := cmd.Run(); err != nil {
		log.Printf("container run failed, err = [%v]", err)
	}
	utils.Must(unix.Unmount("/dev/pts", 0))
	utils.Must(unix.Unmount("/dev", 0))
	utils.Must(unix.Unmount("/sys", 0))
	utils.Must(unix.Unmount("/proc", 0))
	utils.Must(unix.Unmount("/tmp", 0))
}

func copyNameserverConfig(containerID string) error {
	resolvFilePaths := []string{
		"/var/run/systemd/resolve/resolv.conf",
		"/etc/fdockerresolv.conf",
		"/etc/resolv.conf",
	}
	for _, resolvFilePath := range resolvFilePaths {
		if _, err := os.Stat(resolvFilePath); os.IsNotExist(err) {
			continue
		} else {
			return utils.CopyFile(resolvFilePath, workdirs.GetContainerFSHome(containerID)+"/mnt/etc/resolv.conf")
		}
	}
	return nil
}
