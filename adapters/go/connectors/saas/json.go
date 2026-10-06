package saas

import "github.com/SYNEHQ/kelvo-go/provider"

func decodeDocument(raw []byte, destination any) error {
	return provider.DecodeDocument(raw, destination, maxBytes)
}
