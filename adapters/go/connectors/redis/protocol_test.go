package redis

import (
	"bufio"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func TestRESPRejectsUntrustedLengthsBeforeAllocation(t *testing.T) {
	cases := []struct {
		wire   string
		limit  int64
		values int
	}{
		{"$1073741824\r\n", 1024, 32}, {"*1000000000\r\n", 1024, 32}, {"$-2\r\n", 1024, 32},
		{"*2\r\n:1\r\n:2\r\n", 1024, 2}, {strings.Repeat("*1\r\n", 18) + ":1\r\n", 1024, 32},
		{"$2\r\nabZZ", 1024, 32}, {"+" + strings.Repeat("a", 4096) + "\r\n", 10000, 32},
	}
	for _, c := range cases {
		_, err := readReply(bufio.NewReaderSize(strings.NewReader(c.wire), 4096), &replyBudget{bytes: c.limit, values: c.values}, 0)
		if err == nil {
			t.Fatal("unsafe frame accepted", len(c.wire))
		}
	}
}
func TestRESPPreservesBinaryNilAndExactInteger(t *testing.T) {
	got, err := readReply(bufio.NewReader(strings.NewReader("*4\r\n$2\r\n\x00\xff\r\n$-1\r\n:9223372036854775807\r\n+OK\r\n")), &replyBudget{bytes: 1024, values: 8}, 0)
	want := []any{[]byte{0, 255}, nil, int64(9223372036854775807), "OK"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
}
func TestRESPErrorDoesNotExposeServerDiagnostic(t *testing.T) {
	_, err := readReply(bufio.NewReader(strings.NewReader("-ERR private-source-password\r\n")), &replyBudget{bytes: 1024, values: 8}, 0)
	var server serverError
	if !errors.As(err, &server) || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	if _, err := readReply(bufio.NewReader(strings.NewReader("$50\r\n")), &replyBudget{bytes: 8, values: 8}, 0); !errors.Is(err, adapter.ErrLimit) {
		t.Fatal(err)
	}
}
