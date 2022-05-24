package network

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"path"

	"github.com/shuveb/containers-the-hard-way/utils"
	"github.com/shuveb/containers-the-hard-way/workdirs"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

type Accessor struct{}

func GetAccessor() Accessor {
	return Accessor{}
}

/*
	Go through the list of interfaces and return true if the fdocker0 bridge is up
*/

func (n Accessor) IsFDockerBridgeUp() (bool, error) {
	if links, err := netlink.LinkList(); err != nil {
		log.Printf("Unable to get list of links.\n")
		return false, err
	} else {
		for _, link := range links {
			if link.Type() == "bridge" && link.Attrs().Name == "fdocker0" {
				return true, nil
			}
		}
		return false, err
	}
}

/*
	This function sets up the "fdocker0" bridge, which is our main bridge
	interface. To keep things simple, we assign the hopefully unassigned
	and obscure private IP 172.31.0.1 to it, which is from the range of
	IPs which we will also use for our containers.
*/

func (n Accessor) SetupFDockerBridge() error {
	linkAttrs := netlink.NewLinkAttrs()
	linkAttrs.Name = "fdocker0"
	fdockerBridge := &netlink.Bridge{LinkAttrs: linkAttrs}
	if err := netlink.LinkAdd(fdockerBridge); err != nil {
		return err
	}
	addr, _ := netlink.ParseAddr("172.31.0.1/16")
	netlink.AddrAdd(fdockerBridge, addr)
	netlink.LinkSetUp(fdockerBridge)
	return nil
}

func (n Accessor) SetupVirtualEthOnHost(containerID string) error {
	veth0 := "veth0_" + containerID[:6]
	veth1 := "veth1_" + containerID[:6]
	linkAttrs := netlink.NewLinkAttrs()
	linkAttrs.Name = veth0
	veth0Struct := &netlink.Veth{
		LinkAttrs:        linkAttrs,
		PeerName:         veth1,
		PeerHardwareAddr: n.createMACAddress(),
	}
	if err := netlink.LinkAdd(veth0Struct); err != nil {
		return err
	}
	netlink.LinkSetUp(veth0Struct)
	fdockerBridge, _ := netlink.LinkByName("fdocker0")
	netlink.LinkSetMaster(veth0Struct, fdockerBridge)

	return nil
}

func (n Accessor) SetupContainerNetworkInterface(containerID string) {
	n.setupContainerNetworkInterfaceStep1(containerID)
	n.setupContainerNetworkInterfaceStep2(containerID)
}

func (n Accessor) setupContainerNetworkInterfaceStep1(containerID string) {
	nsMount := n.getNetNsPath(containerID)

	fd, err := unix.Open(nsMount, unix.O_RDONLY, 0)
	defer unix.Close(fd)
	if err != nil {
		log.Fatalf("Unable to open: %v\n", err)
	}
	/* Set veth1 of the new container to the new network namespace */
	veth1 := "veth1_" + containerID[:6]
	veth1Link, err := netlink.LinkByName(veth1)
	if err != nil {
		log.Fatalf("Unable to fetch veth1: %v\n", err)
	}
	if err := netlink.LinkSetNsFd(veth1Link, fd); err != nil {
		log.Fatalf("Unable to set network namespace for veth1: %v\n", err)
	}
}

func (n Accessor) setupContainerNetworkInterfaceStep2(containerID string) {
	nsMount := n.getNetNsPath(containerID)
	fd, err := unix.Open(nsMount, unix.O_RDONLY, 0)
	defer func() {
		_ = unix.Close(fd)
	}()
	if err != nil {
		log.Fatalf("Unable to open: %v\n", err)
	}
	if err := unix.Setns(fd, unix.CLONE_NEWNET); err != nil {
		log.Fatalf("Setns system call failed: %v\n", err)
	}

	veth1 := "veth1_" + containerID[:6]
	veth1Link, err := netlink.LinkByName(veth1)
	if err != nil {
		log.Fatalf("Unable to fetch veth1: %v\n", err)
	}
	addr, _ := netlink.ParseAddr(n.createIPAddress() + "/16")
	if err := netlink.AddrAdd(veth1Link, addr); err != nil {
		log.Fatalf("Error assigning IP to veth1: %v\n", err)
	}

	/* Bring up the interface */
	utils.MustWithMsg(netlink.LinkSetUp(veth1Link), "Unable to bring up veth1")

	/* Add a default route */
	route := netlink.Route{
		Scope:     netlink.SCOPE_UNIVERSE,
		LinkIndex: veth1Link.Attrs().Index,
		Gw:        net.ParseIP("172.31.0.1"),
		Dst:       nil,
	}
	utils.MustWithMsg(netlink.RouteAdd(&route), "Unable to add default route")
}

/*
	This is the function that sets the IP address for the local interface.
	There seems to be a bug in the netlink library in that it does not
	succeed in looking up the local interface by name, always returning an
	error. As a workaround, we loop through the interfaces, compare the name,
	set the IP and make the interface up.

*/

func (n Accessor) SetupLocalInterface() {
	links, _ := netlink.LinkList()
	for _, link := range links {
		if link.Attrs().Name == "lo" {
			loAddr, _ := netlink.ParseAddr("127.0.0.1/32")
			if err := netlink.AddrAdd(link, loAddr); err != nil {
				log.Println("Unable to configure local interface!")
			}
			netlink.LinkSetUp(link)
		}
	}
}

func (n Accessor) SetupNewNetworkNamespace(containerID string) {
	_ = utils.EnsureDirs([]string{workdirs.NetNsPath()})
	nsMount := n.getNetNsPath(containerID)
	if _, err := unix.Open(nsMount, unix.O_RDONLY|unix.O_CREAT|unix.O_EXCL, 0644); err != nil {
		log.Fatalf("Unable to open bind mount file: :%v\n", err)
	}

	fd, err := unix.Open("/proc/self/ns/net", unix.O_RDONLY, 0)
	defer unix.Close(fd)
	if err != nil {
		log.Fatalf("Unable to open: %v\n", err)
	}

	if err := unix.Unshare(unix.CLONE_NEWNET); err != nil {
		log.Fatalf("Unshare system call failed: %v\n", err)
	}
	if err := unix.Mount("/proc/self/ns/net", nsMount, "bind", unix.MS_BIND, ""); err != nil {
		log.Fatalf("Mount system call failed: %v\n", err)
	}
	if err := unix.Setns(fd, unix.CLONE_NEWNET); err != nil {
		log.Fatalf("Setns system call failed: %v\n", err)
	}
}

func (n Accessor) JoinContainerNetworkNamespace(containerID string) error {
	nsMount := n.getNetNsPath(containerID)
	fd, err := unix.Open(nsMount, unix.O_RDONLY, 0)
	if err != nil {
		log.Printf("Unable to open: %v\n", err)
		return err
	}
	if err := unix.Setns(fd, unix.CLONE_NEWNET); err != nil {
		log.Printf("Setns system call failed: %v\n", err)
		return err
	}
	return nil
}

func (n Accessor) createMACAddress() net.HardwareAddr {
	hw := make(net.HardwareAddr, 6)
	hw[0] = 0x02
	hw[1] = 0x42
	rand.Read(hw[2:])
	return hw
}

func (n Accessor) createIPAddress() string {
	byte1 := rand.Intn(254)
	byte2 := rand.Intn(254)
	return fmt.Sprintf("172.31.%d.%d", byte1, byte2)
}

func (n Accessor) UnmountNetworkNamespace(containerID string) {
	netNsPath := n.getNetNsPath(containerID)
	if err := unix.Unmount(netNsPath, 0); err != nil {
		log.Fatalf("Uable to mount network namespace: %v at %s", err, netNsPath)
	}
}

func (n Accessor) getNetNsPath(containerID string) string {
	return path.Join(workdirs.NetNsPath(), containerID)
}
