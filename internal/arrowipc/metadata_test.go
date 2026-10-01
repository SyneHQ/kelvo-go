package arrowipc

import (
	"errors"
	"testing"
)

func TestRejectsMalformedMetadata(t *testing.T) {
	for _, b := range [][]byte{nil, {}, {1, 2, 3, 4}, make([]byte, 32)} {
		if _, err := ValidateMessageMetadata(b); err == nil || (!errors.Is(err, ErrInvalid) && !errors.Is(err, ErrLimit)) {
			t.Fatalf("unexpected %v", err)
		}
	}
}
