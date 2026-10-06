// Package redis implements the bounded RESP2 command surface used by Kelvo's
// isolated adapter. Lengths are checked before allocating server-owned payloads.
package redis

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

type serverError struct{}

func (serverError) Error() string { return "Redis rejected command" }

type replyBudget struct {
	bytes  int64
	values int
}

func readReply(reader *bufio.Reader, budget *replyBudget, depth int) (any, error) {
	if depth > 16 || budget.values <= 0 || budget.bytes <= 0 {
		return nil, adapter.ErrLimit
	}
	budget.values--
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) < 3 || line[len(line)-2] != '\r' || int64(len(line)) > budget.bytes {
		return nil, adapter.ErrLimit
	}
	budget.bytes -= int64(len(line))
	prefix, value := line[0], line[1:len(line)-2]
	switch prefix {
	case '+':
		return string(value), nil
	case '-':
		return nil, serverError{}
	case ':':
		n, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		return n, nil
	case '$', '*':
		n, err := strconv.ParseInt(string(value), 10, 32)
		if err != nil || n < -1 {
			return nil, adapter.ErrInvalid
		}
		if n == -1 {
			return nil, nil
		}
		if prefix == '$' {
			if n+2 > budget.bytes {
				return nil, adapter.ErrLimit
			}
			raw := make([]byte, int(n)+2)
			if _, err := io.ReadFull(reader, raw); err != nil {
				return nil, err
			}
			if raw[n] != '\r' || raw[n+1] != '\n' {
				return nil, adapter.ErrInvalid
			}
			budget.bytes -= n + 2
			return raw[:n], nil
		}
		if n > int64(budget.values) || n*3 > budget.bytes {
			return nil, adapter.ErrLimit
		}
		items := make([]any, int(n))
		for i := range items {
			items[i], err = readReply(reader, budget, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return items, nil
	default:
		return nil, adapter.ErrInvalid
	}
}

func (s *Session) command(ctx context.Context, args []string, maxBytes int64, maxValues int) (any, error) {
	if ctx == nil || ctx.Err() != nil || s.conn == nil || len(args) == 0 {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, adapter.ErrInvalid
	}
	deadline := time.Now().Add(30 * time.Second)
	if until, ok := ctx.Deadline(); ok && until.Before(deadline) {
		deadline = until
	}
	if err := s.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	conn := s.conn
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var raw bytes.Buffer
	fmt.Fprintf(&raw, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&raw, "$%d\r\n", len(arg))
		raw.WriteString(arg)
		raw.WriteString("\r\n")
	}
	if raw.Len() > 2<<20 {
		return nil, adapter.ErrLimit
	}
	if _, err := io.Copy(s.conn, &raw); err != nil {
		_ = s.conn.Close()
		return nil, err
	}
	value, err := readReply(s.reader, &replyBudget{bytes: maxBytes, values: maxValues}, 0)
	if err != nil {
		_ = s.conn.Close()
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return value, err
}

func redisString(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	}
	return "", false
}
