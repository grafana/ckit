// Package advertise provide utilities to find addresses to advertise to
// cluster peers.
package advertise

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// DefaultInterfaces is a default list of common interfaces that are used for
// local network traffic for Unix-like platforms.
var DefaultInterfaces = []string{"eth0", "en0"}

// FirstAddress returns the first IPv4/IPv6 address from the given interface names.
// Link-local unicast addresses will be ignored if possible.
func FirstAddress(interfaces []string) (string, error) {
	return firstAddress(interfaces, getInterfaceAddresses, net.Interfaces)
}

// NetworkInterfaceAddressGetter matches the signature of net.InterfaceByName() to allow for test mocks.
type NetworkInterfaceAddressGetter func(name string) ([]netip.Addr, error)

// InterfaceLister matches the signature of net.Interfaces() to allow for test mocks.
type InterfaceLister func() ([]net.Interface, error)

// FirstAddress returns the first IPv4/IPv6 address from the given interface names.
// Link-local unicast addresses will be ignored if possible.
func firstAddress(interfaces []string, interfaceAddrsFunc NetworkInterfaceAddressGetter, interfaceLister InterfaceLister) (string, error) {
	var (
		errs   *multierror.Error
		bestIP netip.Addr
	)

	if len(interfaces) == 1 && interfaces[0] == "all" {
		infs, err := interfaceLister()
		if err != nil {
			return "", fmt.Errorf("failed to get interface list: %w", err)
		}
		interfaces = make([]string, len(infs))
		for i, v := range infs {
			interfaces[i] = v.Name
		}
	}

	for _, ifaceName := range interfaces {
		addrs, err := interfaceAddrsFunc(ifaceName)
		if err != nil {
			err = fmt.Errorf("interface %q: %w", ifaceName, err)
			errs = multierror.Append(errs, err)
			continue
		}

		canditate := filterBestIP(addrs)
		if !canditate.IsValid() {
			continue
		}

		if canditate.Is4() && !canditate.IsLinkLocalUnicast() {
			// Best address possible, we can return early.
			return canditate.String(), nil
		}

		bestIP = filterBestIP([]netip.Addr{canditate, bestIP})
	}
	if !bestIP.IsValid() {
		if errs != nil {
			return "", errors.Wrapf(errs, "no useable address found for interfaces %v", interfaces)
		} else {
			return "", fmt.Errorf("no useable address found for interfaces %v", interfaces)
		}
	}
	return bestIP.String(), nil
}

// getInterfaceAddresses is the standard approach to collecting []net.Addr from a network interface by name.
func getInterfaceAddresses(name string) ([]netip.Addr, error) {
	inf, err := net.InterfaceByName(name)
	if err != nil {
		return nil, err
	}

	addrs, err := inf.Addrs()
	if err != nil {
		return nil, err
	}

	// Using netip.Addr to allow for easier and consistent address parsing.
	// Without this, the net.ParseCIDR() that we might like to use in a test does
	// not have the same net.Addr implementation that we get from calling
	// interface.Addrs() as above.  Here we normalize on netip.Addr.
	netaddrs := make([]netip.Addr, len(addrs))
	for i, a := range addrs {
		prefix, err := netip.ParsePrefix(a.String())
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse netip.Prefix")
		}
		netaddrs[i] = prefix.Addr()
	}

	return netaddrs, nil
}

// filterBestIP returns an opinionated "best" address from a list of addresses.
// The ordering is the following:
// - IPv4 valid and not link-local unicast
// - IPv6 valid and not link-local unicast
// - IPv4 valid and link-local unicast
// - IPv6 valid and link-local unicast
// If none of the above are found, an invalid address is returned.
// Loopback addresses are never selected.
func filterBestIP(addrs []netip.Addr) netip.Addr {
	var invalid, inet4Addr, inet6Addr netip.Addr

	for _, addr := range addrs {
		if addr.IsLoopback() || !addr.IsValid() {
			continue
		}

		if addr.Is4() {
			// If we have already been set, can we improve on the quality?
			if inet4Addr.IsValid() {
				if inet4Addr.IsLinkLocalUnicast() && !addr.IsLinkLocalUnicast() {
					inet4Addr = addr
				}
				continue
			}
			inet4Addr = addr
		}

		if addr.Is6() {
			// If we have already been set, can we improve on the quality?
			if inet6Addr.IsValid() {
				if inet6Addr.IsLinkLocalUnicast() && !addr.IsLinkLocalUnicast() {
					inet6Addr = addr
				}
				continue
			}
			inet6Addr = addr
		}
	}

	// If both address families have been set, compare.
	if inet4Addr.IsValid() && inet6Addr.IsValid() {
		if inet4Addr.IsLinkLocalUnicast() && !inet6Addr.IsLinkLocalUnicast() {
			return inet6Addr
		}
		if inet6Addr.IsLinkLocalUnicast() && !inet4Addr.IsLinkLocalUnicast() {
			return inet4Addr
		}
		return inet4Addr
	}

	if inet4Addr.IsValid() {
		return inet4Addr
	}

	if inet6Addr.IsValid() {
		return inet6Addr
	}

	return invalid
}
