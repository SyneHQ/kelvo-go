// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import "crypto/sha256"

// Version 2 is deliberately exclusive with version 1 tenant-only keys.
// Every provisioned tenant must be represented, including a disabled empty map.
func parsePrincipalKeys(document gatewayKeyDocument, tenants map[string]bool, result gatewayKeySet) (gatewayKeySet, error) {
	bad := func() (gatewayKeySet, error) { return gatewayKeySet{}, errGatewayAuthUnavailable }
	if document.Tenants != nil || len(document.Principals) != len(tenants) {
		return bad()
	}
	for tenant, principals := range document.Principals {
		if !tenants[tenant] || principals == nil || len(principals) > 64 {
			return bad()
		}
		for id, keys := range principals {
			if !clusterID.MatchString(id) || keys == nil || len(keys) > gatewayMaxTenantKeys {
				return bad()
			}
			for _, key := range keys {
				if len(key) < 32 || len(key) > 256 {
					return bad()
				}
				for _, ch := range []byte(key) {
					if ch < 33 || ch > 126 || ch == 44 {
						return bad()
					}
				}
				digest := sha256.Sum256([]byte(key))
				if _, duplicate := result.keys[digest]; duplicate {
					return bad()
				}
				result.keys[digest], result.principals[digest] = tenant, id
			}
		}
	}
	return result, nil
}
