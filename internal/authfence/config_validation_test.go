// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import "testing"

func TestFenceConfigValidationIsInertAndStrict(t *testing.T) {
	config := protocolConfig(t, "east")
	// These paths and environment references need not exist at configuration time.
	config.CertFile, config.KeyFile = "/missing/authority-cert.pem", "/missing/authority-key.pem"
	if err := config.Validate(); err != nil {
		t.Fatal("reference-only configuration validation opened resources", err)
	}
	for name, mutate := range map[string]func(*Config){
		"plaintext":   func(c *Config) { c.URL = "nats://localhost:4222" },
		"URL secret":  func(c *Config) { c.URL = "tls://user:secret@localhost:4222" },
		"relative CA": func(c *Config) { c.CAFile = "ca.pem" },
		"no CA":       func(c *Config) { c.CAFile = "" },
		"partial TLS": func(c *Config) { c.KeyFile = "" },
		"mixed auth":  func(c *Config) { c.CredentialsFile = "/missing/authority.creds" },
		"invalid env": func(c *Config) { c.PasswordEnv = "INVALID-ENV" },
		"no scope":    func(c *Config) { c.Scope = Scope{} },
		"no replica":  func(c *Config) { c.ReplicaID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			c := config
			mutate(&c)
			if err := c.Validate(); err != ErrInvalid {
				t.Fatal("malformed authority configuration accepted", err)
			}
		})
	}
	config.Username, config.PasswordEnv, config.CredentialsFile = "", "", "/missing/authority.creds"
	if err := config.Validate(); err != nil {
		t.Fatal("credential reference requires runtime I/O", err)
	}
}
