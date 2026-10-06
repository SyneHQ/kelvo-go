package migration

import "regexp"

var serverUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ValidServerUUID requires the canonical, nonempty server identity used in an
// operator's single-node ClickHouse migration binding.
func ValidServerUUID(value string) bool {
	return serverUUID.MatchString(value) && value != "00000000-0000-0000-0000-000000000000"
}
