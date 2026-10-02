// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import "testing"

type capabilityFixture struct{ value Capabilities }

func (c *capabilityFixture) FederationCapabilities() Capabilities { return c.value }

type hiddenCapabilityWrapper struct{ inner *capabilityFixture }
type forwardingCapabilityWrapper struct{ inner *capabilityFixture }

func (w forwardingCapabilityWrapper) FederationCapabilities() Capabilities {
	return w.inner.FederationCapabilities()
}

type panickingCapability struct{}

func (panickingCapability) FederationCapabilities() Capabilities { panic("private driver error") }
func TestCapabilitiesAreOptionalValidatedAndExplicitlyForwarded(t *testing.T) {
	driver := &capabilityFixture{Capabilities{Version: 1, Projection: true, Comparisons: []ComparisonCapability{{Type: "int64", Operators: []string{"eq", "gt"}}}}}
	got, known := InspectCapabilities(driver)
	if !known || !got.Projection {
		t.Fatal("valid capabilities missing")
	}
	got.Comparisons[0].Operators[0] = "ge"
	if driver.value.Comparisons[0].Operators[0] != "eq" {
		t.Fatal("capabilities alias adapter state")
	}
	for _, provider := range []any{nil, hiddenCapabilityWrapper{driver}, panickingCapability{}, &capabilityFixture{Capabilities{Version: 99, Projection: true}}, &capabilityFixture{Capabilities{Version: 1, Comparisons: []ComparisonCapability{{Type: "text", Operators: []string{"eq"}}}}}} {
		capabilities, known := InspectCapabilities(provider)
		if known || capabilities.Version != 0 || capabilities.Projection {
			t.Fatal("unknown declaration advertised support")
		}
	}
	if _, known := InspectCapabilities(forwardingCapabilityWrapper{driver}); !known {
		t.Fatal("forwarded declaration missing")
	}
}
func TestCapabilitiesRejectAmbiguousDeclarations(t *testing.T) {
	for _, capabilities := range []Capabilities{
		{Version: 1, Comparisons: []ComparisonCapability{{Type: "int64", Operators: []string{"eq", "eq"}}}},
		{Version: 1, Comparisons: []ComparisonCapability{{Type: "int64", Operators: []string{"eq"}}, {Type: "int64", Operators: []string{"gt"}}}},
		{Version: 1, Comparisons: []ComparisonCapability{{Type: "int64", Operators: []string{"SQL injection"}}}},
		{Version: 1, Comparisons: []ComparisonCapability{{Type: "int64"}}},
	} {
		if capabilities.Validate() == nil {
			t.Fatal("ambiguous capabilities accepted")
		}
	}
}
