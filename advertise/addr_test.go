package advertise

import (
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Mock function to simulate various interface address conditions
func mockAddressGetter(data map[string][]string) NetworkInterfaceAddressGetter {
	return func(name string) ([]netip.Addr, error) {
		if addrs, found := data[name]; found {
			var netAddrs []netip.Addr
			for _, a := range addrs {
				prefix, _ := netip.ParsePrefix(a)
				netAddrs = append(netAddrs, prefix.Addr())
			}
			return netAddrs, nil
		}
		return nil, errors.New("interface not found")
	}
}

func mockInterfaceLister() ([]net.Interface, error) {
	return []net.Interface{
		{Name: "eth0"},
		{Name: "eth1"},
		{Name: "eth2"},
		{Name: "lo"},
	}, nil
}

func TestFirstAddress(t *testing.T) {
	tests := []struct {
		name          string
		interfaceData map[string][]string
		interfaces    []string
		expected      string
		expectedError string
	}{
		{
			name: "Multiple interfaces, one with valid IPv4",
			interfaceData: map[string][]string{
				"eth0": {"::2/128"},
				"eth1": {"192.168.1.1/24"},
			},
			interfaces: []string{"eth0", "eth1"},
			expected:   "192.168.1.1",
		},
		{
			name: "Multiple interfaces, all with IPv6",
			interfaceData: map[string][]string{
				"eth0": {"::2/128"},
				"eth1": {"::3/128"},
			},
			interfaces: []string{"eth0", "eth1"},
			expected:   "::3",
		},
		{
			name: "Invalid interface",
			interfaceData: map[string][]string{
				"eth0": {"192.168.1.1/24"},
			},
			interfaces:    []string{"invalid"},
			expectedError: "no useable address found for interfaces [invalid]: 1 error occurred:\n\t* interface \"invalid\": interface not found\n\n",
		},
		{
			name: "Multiple interfaces, one invalid, one with valid IPv4",
			interfaceData: map[string][]string{
				"eth0": {"::2/128"},
				"eth1": {"192.168.1.1/24"},
			},
			interfaces: []string{"invalid", "eth0", "eth1"},
			expected:   "192.168.1.1",
		},
		{
			name: "Empty interfaces",
			interfaceData: map[string][]string{
				"eth0": {},
				"eth1": {},
			},
			interfaces:    []string{"eth0", "eth1"},
			expectedError: "no useable address found for interfaces [eth0 eth1]",
		},
		{
			name:          "No interfaces",
			interfaceData: map[string][]string{},
			interfaces:    []string{"eth0", "eth1"},
			expectedError: "no useable address found for interfaces [eth0 eth1]: 2 errors occurred:\n\t* interface \"eth0\": interface not found\n\t* interface \"eth1\": interface not found\n\n",
		},
		{
			name: "Ignore loopback addresses",
			interfaceData: map[string][]string{
				"eth0": {"127.0.0.1/8", "192.168.1.1/24"},
			},
			interfaces: []string{"eth0"},
			expected:   "192.168.1.1",
		},
		{
			name: "Ignore IPv6 loopback addresses",
			interfaceData: map[string][]string{
				"eth0": {"::1/128", "::2/128"},
			},
			interfaces: []string{"eth0"},
			expected:   "::2",
		},
		{
			name: "Ignore link-local unicast if possible (IPv4)",
			interfaceData: map[string][]string{
				"eth0": {"169.254.0.1/16", "192.168.1.1/24"},
			},
			interfaces: []string{"eth0"},
			expected:   "192.168.1.1",
		},
		{
			name: "Ignore link-local unicast if possible (IPv6)",
			interfaceData: map[string][]string{
				"eth0": {"fe80::1/64", "::2/128"},
			},
			interfaces: []string{"eth0"},
			expected:   "::2",
		},
		{
			name: "Use link-local unicast if no other option (IPv4)",
			interfaceData: map[string][]string{
				"eth0": {"169.254.0.1/16"},
			},
			interfaces: []string{"eth0"},
			expected:   "169.254.0.1",
		},
		{
			name: "Use link-local unicast if no other option (IPv6)",
			interfaceData: map[string][]string{
				"eth0": {"fe80::1/64"},
			},
			interfaces: []string{"eth0"},
			expected:   "fe80::1",
		},
		{
			name: "Select from all interfaces",
			interfaceData: map[string][]string{
				"eth0": {"192.168.1.1/24"},
				"eth1": {"10.0.0.2/24"},
				"eth2": {"169.254.0.1/16"},
				"lo":   {"127.0.0.1/8"},
			},
			interfaces: []string{"all"},
			expected:   "192.168.1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFunc := mockAddressGetter(tt.interfaceData)
			result, err := firstAddress(tt.interfaces, mockFunc, mockInterfaceLister)
			if tt.expectedError != "" {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err.Error())
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
