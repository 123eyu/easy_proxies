package config

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

func normalizePoolMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "sequential", "round-robin", "round_robin":
		return "sequential", nil
	case "random":
		return "random", nil
	case "balance":
		return "balance", nil
	case "latency":
		return "latency", nil
	default:
		return "", fmt.Errorf("unsupported pool.mode (use sequential, random, balance or latency)")
	}
}

// ValidateManagementSettings validates settings without fetching subscriptions,
// resolving DNS, or probing ports. Zero values retain documented defaults.
func (c *Config) ValidateManagementSettings() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	switch c.Mode {
	case "", "pool", "multi-port", "multi_port", "hybrid":
	default:
		return fmt.Errorf("invalid proxy mode")
	}
	if _, err := normalizePoolMode(c.Pool.Mode); err != nil {
		return err
	}
	for name, value := range map[string]string{"listener.address": c.Listener.Address, "multi_port.address": c.MultiPort.Address, "geoip.listen": c.GeoIP.Listen} {
		if value != "" {
			if _, err := netip.ParseAddr(value); err != nil {
				return fmt.Errorf("%s must be an IPv4 or IPv6 address", name)
			}
		}
	}
	if c.Management.Listen != "" {
		_, port, err := net.SplitHostPort(c.Management.Listen)
		number, parseErr := strconv.Atoi(port)
		if err != nil || parseErr != nil || number < 1 || number > 65535 {
			return fmt.Errorf("management.listen must be host:port with a port from 1 to 65535")
		}
	}
	if c.Pool.FailureThreshold < 0 || c.Pool.RetryAttempts < 0 {
		return fmt.Errorf("pool failure threshold and retry attempts must not be negative")
	}
	if c.Pool.BlacklistDuration < 0 || c.Pool.TransientCooldown < 0 || c.Pool.RateLimitCooldown < 0 {
		return fmt.Errorf("pool durations must not be negative")
	}
	if c.Management.ProbeConcurrency < 0 || c.Management.ProbeConcurrency > 1024 {
		return fmt.Errorf("management.probe_concurrency must be from 1 to 1024 or 0 for default")
	}
	if c.Sticky.SessionTTL < 0 || c.Sticky.SessionTTL > MaxSessionTTL {
		return fmt.Errorf("sticky.session_ttl must be positive and at most %s", MaxSessionTTL)
	}
	if c.Sticky.MaxSessions < 0 || c.Sticky.MaxSessions > MaxSessionCapacity {
		return fmt.Errorf("sticky.max_sessions must be between 1 and %d", MaxSessionCapacity)
	}
	if c.Sticky.Enabled && (c.Mode == "pool" || c.Mode == "hybrid" || c.Mode == "") {
		listenPort := c.Listener.Port
		if listenPort == 0 {
			listenPort = 2323
		}
		if c.Sticky.Port != 0 && c.Sticky.Port == listenPort {
			return fmt.Errorf("sticky.port conflicts with listener.port")
		}
		if c.Sticky.Port == 0 && listenPort == 65535 {
			return fmt.Errorf("set an explicit sticky.port when listener.port is 65535")
		}
	}
	if c.GeoIP.Enabled && c.GeoIP.Port > 0 && c.GeoIP.Port == c.Sticky.Port && c.Sticky.Enabled {
		return fmt.Errorf("geoip.port conflicts with sticky.port")
	}
	return nil
}
