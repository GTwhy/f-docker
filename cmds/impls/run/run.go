package run

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

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
	return "run"
}

func (e Executor) Implicit() bool {
	return false
}

func (e Executor) Usage() string {
	return "f-docker run [--mem] [--swap] [--pids] [--cpus] <image> <command>"
}

func (e Executor) Exec() {
	runArgs := parseFlags()
	setUpBridge()
	initContainer(runArgs)
}

type runArgs struct {
	mem        int
	swap       int
	pids       int
	cpus       float64
	read_bps   string
	read_iops  string
	write_bps  string
	write_iops string
	imageName  string
	commands   []string
}

func parseFlags() *runArgs {
	fs := flag.FlagSet{}
	fs.ParseErrorsWhitelist.UnknownFlags = true

	mem := fs.Int("mem", -1, "Max RAM to allow in MB")
	swap := fs.Int("swap", -1, "Max swap to allow in MB")
	pids := fs.Int("pids", -1, "Number of max processes to allow")
	cpus := fs.Float64("cpus", -1, "Number of CPU cores to restrict to")
	read_bps := fs.String("device-read-bps", "", "Max rate to read from dev")
	read_iops := fs.String("device-read-iops", "", "Max times to read from dev in one second")
	write_bps := fs.String("device-write-bps", "", "Number of max rate to write into the dev")
	write_iops := fs.String("device-write-iops", "", "Max times to write into the dev in one second")
	if err := fs.Parse(os.Args[2:]); err != nil {
		fmt.Println("Error parsing: ", err)
	}
	if len(fs.Args()) < 2 {
		log.Fatalf("Please pass image name and command to run")
	}
	return &runArgs{
		mem:        *mem,
		swap:       *swap,
		pids:       *pids,
		cpus:       *cpus,
		read_bps:   *read_bps,
		read_iops:  *read_iops,
		write_bps:  *write_bps,
		write_iops: *write_iops,
		imageName:  fs.Args()[0],
		commands:   fs.Args()[1:],
	}
}

func setUpBridge() {
	accessor := network.GetAccessor()
	if isUp, _ := accessor.IsFDockerBridgeUp(); !isUp {
		log.Println("Bringing up the fdocker0 bridge...")
		if err := accessor.SetupFDockerBridge(); err != nil {
			log.Fatalf("Unable to create fdocker0 bridge: %v", err)
		}
	}
}

func createContainerID() string {
	randBytes := make([]byte, 6)
	rand.Read(randBytes)
	return fmt.Sprintf("%02x%02x%02x%02x%02x%02x",
		randBytes[0], randBytes[1], randBytes[2],
		randBytes[3], randBytes[4], randBytes[5])
}

func getContainerMntPath(containerID string) string {
	return path.Join(workdirs.GetContainerFSHome(containerID), "mnt")
}

func getContainerUpperDirPath(containerID string) string {
	return path.Join(workdirs.GetContainerFSHome(containerID), "upperdir")
}

func getContainerWorkDirPath(containerID string) string {
	return path.Join(workdirs.GetContainerFSHome(containerID), "workdir")
}

func createContainerDirectories(containerID string) {
	contDirs := []string{
		workdirs.GetContainerFSHome(containerID),
		getContainerMntPath(containerID),
		getContainerUpperDirPath(containerID),
		getContainerWorkDirPath(containerID)}
	if err := utils.EnsureDirs(contDirs); err != nil {
		log.Fatalf("Unable to create required directories: %v\n", err)
	}
}

func mountOverlayFileSystem(containerID string, imageShaHex string) {
	var srcLayers []string
	accessor := image.GetAccessor()
	pathManifest := accessor.GetManifestPathForImage(imageShaHex)
	mani := accessor.ParseManifest(pathManifest)
	imageBasePath := accessor.GetBasePathForImage(imageShaHex)
	for _, layer := range mani.Layers {
		srcLayers = append([]string{imageBasePath + "/" + layer[:12] + "/fs"}, srcLayers...)
	}
	contFSHome := workdirs.GetContainerFSHome(containerID)
	mntOptions := "lowerdir=" + strings.Join(srcLayers, ":") + ",upperdir=" + contFSHome + "/upperdir,workdir=" + contFSHome + "/workdir"
	//log.Printf("mntOptions=[%s]", mntOptions)
	//log.Printf("contFSHome mnt=[%s]", contFSHome+"/mnt")
	if err := unix.Mount("/dev/sda5", contFSHome+"/mnt", "overlay", 0, mntOptions); err != nil {
		log.Fatalf("Mount failed: %v\n", err)
	}
}

func unmountNetworkNamespace(containerID string) {
	accessor := network.GetAccessor()
	accessor.UnmountNetworkNamespace(containerID)
}

func unmountContainerFs(containerID string) {
	path.Join(workdirs.ContainersPath(), containerID, "fs", "mnt")
	mountedPath := workdirs.ContainersPath() + "/" + containerID + "/fs/mnt"
	if err := unix.Unmount(mountedPath, 0); err != nil {
		log.Fatalf("Uable to mount container file system: %v at %s", err, mountedPath)
	}
}

func prepareAndExecuteContainer(mem int, swap int, pids int, cpus float64, read_bps string, read_iops string, write_bps string, write_iops string,
	containerID string, imageShaHex string, cmdArgs []string) {

	/* Setup the network namespace  */
	cmd := &exec.Cmd{
		Path:   "/proc/self/exe",
		Args:   []string{"/proc/self/exe", "setup-netns", containerID},
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	utils.Must(cmd.Run())

	/* Namespace and setup the virtual interface  */
	cmd = &exec.Cmd{
		Path:   "/proc/self/exe",
		Args:   []string{"/proc/self/exe", "setup-veth", containerID},
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	utils.Must(cmd.Run())
	/*
		From namespaces(7)
		       Namespace Flag            Isolates
		       --------- ----   		 --------
		       Cgroup    CLONE_NEWCGROUP Cgroup root directory
		       IPC       CLONE_NEWIPC    System V IPC,
		                                 POSIX message queues
		       Network   CLONE_NEWNET    Network devices,
		                                 stacks, ports, etc.
		       Mount     CLONE_NEWNS     Mount points
		       PID       CLONE_NEWPID    Process IDs
		       Time      CLONE_NEWTIME   Boot and monotonic
		                                 clocks
		       User      CLONE_NEWUSER   User and group IDs
		       UTS       CLONE_NEWUTS    Hostname and NIS
		                                 domain name
	*/
	var opts []string
	if mem > 0 {
		opts = append(opts, "--mem="+strconv.Itoa(mem))
	}
	if swap >= 0 {
		opts = append(opts, "--swap="+strconv.Itoa(swap))
	}
	if pids > 0 {
		opts = append(opts, "--pids="+strconv.Itoa(pids))
	}
	if cpus > 0 {
		opts = append(opts, "--cpus="+strconv.FormatFloat(cpus, 'f', 1, 64))
	}
	if read_bps != "" {
		opts = append(opts, "--device-read-bps="+read_bps)
	}
	if read_iops != "" {
		opts = append(opts, "--device-read-iops="+read_iops)
	}
	if write_bps != "" {
		opts = append(opts, "--device-write-bps="+write_bps)
	}
	if write_iops != "" {
		opts = append(opts, "--device-write-iops="+write_iops)
	}
	opts = append(opts, "--img="+imageShaHex)
	args := append([]string{containerID}, cmdArgs...)
	args = append(opts, args...)
	args = append([]string{"child-mode"}, args...)
	cmd = exec.Command("/proc/self/exe", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &unix.SysProcAttr{
		Cloneflags: unix.CLONE_NEWPID |
			unix.CLONE_NEWNS |
			unix.CLONE_NEWUTS |
			unix.CLONE_NEWIPC,
	}
	utils.Must(cmd.Run())
}

func initContainer(args *runArgs) {
	mem, swap, pids, cpus, src, cmds, read_bps, read_iops, write_bps, write_iops := args.mem, args.swap, args.pids, args.cpus, args.imageName, args.commands, args.read_bps, args.read_iops, args.write_bps, args.write_iops
	containerID := createContainerID()
	log.Printf("New container ID: %s\n", containerID)
	imgAccessor := image.GetAccessor()
	netAccessor := network.GetAccessor()
	cGroupsAccessor := cgroups.GetAccessor()
	imageShaHex := imgAccessor.DownloadImageIfRequired(src)
	log.Printf("Image to overlay mount: %s\n", imageShaHex)
	createContainerDirectories(containerID)
	mountOverlayFileSystem(containerID, imageShaHex)
	if err := netAccessor.SetupVirtualEthOnHost(containerID); err != nil {
		log.Fatalf("Unable to setup Veth0 on host: %v", err)
	}
	prepareAndExecuteContainer(mem, swap, pids, cpus, read_bps, read_iops, write_bps, write_iops, containerID, imageShaHex, cmds)
	log.Printf("Container done.\n")
	unmountNetworkNamespace(containerID)
	unmountContainerFs(containerID)
	cGroupsAccessor.RemoveCGroups(containerID)
	_ = os.RemoveAll(workdirs.ContainersPath() + "/" + containerID)
}
