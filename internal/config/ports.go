package config

import (
	"net"
	"strconv"
)

func (c *Config) reservedListenerPorts() map[uint16]bool {
	ports := make(map[uint16]bool)
	if c.Mode == "pool" || c.Mode == "hybrid" {
		ports[c.Listener.Port] = true
		if c.Sticky.Enabled {
			port := c.Sticky.Port
			if port == 0 && c.Listener.Port < 65535 {
				port = c.Listener.Port + 1
			}
			if port > 0 {
				ports[port] = true
			}
		}
	}
	if c.ManagementEnabled() {
		if _, port, err := net.SplitHostPort(c.Management.Listen); err == nil {
			if value, err := strconv.Atoi(port); err == nil && value > 0 && value <= 65535 {
				ports[uint16(value)] = true
			}
		}
		ports[9092] = true // Optional internal Clash traffic controller.
	}
	if c.GeoIP.Enabled {
		port := c.GeoIP.Port
		if port == 0 {
			port = 1221
		}
		if port == c.Listener.Port {
			port = 1221
			if port == c.Listener.Port && port < 65535 {
				port++
			}
		}
		ports[port] = true
	}
	return ports
}
