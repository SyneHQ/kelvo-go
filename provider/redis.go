package provider

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// RedisQuery is a single native command. The saved connection owns database
// selection; session, scripting, administration and subscription commands are
// deliberately absent from this key-level vocabulary.
type RedisQuery struct {
	Args []string `json:"args"`
}

var redisReads = strings.Fields("PING ECHO GET MGET STRLEN GETRANGE EXISTS TYPE TTL PTTL DBSIZE SCAN KEYS HGET HMGET HGETALL HEXISTS HLEN HKEYS HVALS HSCAN LRANGE LINDEX LLEN SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER SSCAN ZRANGE ZREVRANGE ZRANGEBYSCORE ZREVRANGEBYSCORE ZRANGEBYLEX ZSCORE ZMSCORE ZCARD ZCOUNT ZLEXCOUNT ZRANK ZREVRANK ZSCAN XRANGE XREVRANGE XLEN GETBIT BITCOUNT BITPOS PFCOUNT")
var redisWrites = strings.Fields("SET SETNX SETEX PSETEX MSET MSETNX APPEND INCR INCRBY INCRBYFLOAT DECR DECRBY GETSET GETDEL GETEX DEL UNLINK EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST RENAME RENAMENX HSET HSETNX HMSET HDEL HINCRBY HINCRBYFLOAT LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP LSET LREM LTRIM LINSERT SADD SREM SPOP SMOVE ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREMRANGEBYLEX XADD XDEL XTRIM SETBIT BITOP PFADD PFMERGE")

func RedisCommandKind(command string) (operations.Kind, bool) {
	command = strings.ToUpper(command)
	for _, allowed := range redisReads {
		if command == allowed {
			return operations.NativeRead, true
		}
	}
	for _, allowed := range redisWrites {
		if command == allowed {
			return operations.NativeExecute, true
		}
	}
	return "", false
}

func ParseRedis(raw []byte) (RedisQuery, operations.Kind, error) {
	var q RedisQuery
	if operations.DecodeStrict(raw, &q, operations.MaxRequestBytes) != nil || len(q.Args) < 1 || len(q.Args) > 1024 {
		return q, "", operations.ErrInvalid
	}
	kind, ok := RedisCommandKind(q.Args[0])
	if !ok {
		return q, "", operations.ErrUnsupported
	}
	for _, arg := range q.Args {
		if len(arg) > 64<<10 || !utf8.ValidString(arg) {
			return q, "", operations.ErrInvalid
		}
	}
	return q, kind, nil
}

// ParseRedisText preserves quoted arguments, including spaces and redis-cli's
// documented byte escapes. Semicolons/newlines outside quotes are rejected so
// pasted command batches cannot become a different single command.
func ParseRedisText(text string) (RedisQuery, error) {
	q := RedisQuery{}
	if len(text) == 0 || len(text) > operations.MaxRequestBytes || !utf8.ValidString(text) {
		return q, operations.ErrInvalid
	}
	for pos := 0; pos < len(text); {
		for pos < len(text) && (text[pos] == ' ' || text[pos] == '\t') {
			pos++
		}
		if pos == len(text) {
			break
		}
		var value strings.Builder
		quote := byte(0)
		if text[pos] == '\'' || text[pos] == '"' {
			quote = text[pos]
			pos++
		}
		closed := quote == 0
		for pos < len(text) {
			ch := text[pos]
			if quote == 0 && (ch == ' ' || ch == '\t') {
				break
			}
			pos++
			if quote != 0 && ch == quote {
				closed = true
				break
			}
			if quote == 0 && (ch == ';' || ch == '\r' || ch == '\n' || ch == '\'' || ch == '"') || ch == 0 {
				return q, operations.ErrInvalid
			}
			if ch == '\\' && quote == 0 {
				return q, operations.ErrInvalid
			}
			if ch == '\\' && quote == '\'' {
				if pos < len(text) && text[pos] == '\'' {
					value.WriteByte('\'')
					pos++
					continue
				}
				value.WriteByte(ch)
				continue
			}
			if ch == '\\' && quote == '"' {
				if pos == len(text) {
					return q, operations.ErrInvalid
				}
				ch = text[pos]
				pos++
				switch ch {
				case 'n':
					ch = '\n'
				case 'r':
					ch = '\r'
				case 't':
					ch = '\t'
				case 'b':
					ch = '\b'
				case 'a':
					ch = '\a'
				case 'x':
					if pos+2 > len(text) {
						return q, operations.ErrInvalid
					}
					raw, err := hex.DecodeString(text[pos : pos+2])
					if err != nil {
						return q, operations.ErrInvalid
					}
					ch = raw[0]
					pos += 2
				case '\\', '\'', '"', ' ':
				default:
					return q, operations.ErrInvalid
				}
			}
			value.WriteByte(ch)
		}
		if !closed || pos < len(text) && text[pos] != ' ' && text[pos] != '\t' {
			return q, operations.ErrInvalid
		}
		if !utf8.ValidString(value.String()) || value.Len() > 64<<10 {
			return q, operations.ErrInvalid
		}
		q.Args = append(q.Args, value.String())
		if len(q.Args) > 1024 {
			return q, operations.ErrInvalid
		}
	}
	raw, err := json.Marshal(q)
	if err != nil {
		return q, operations.ErrInvalid
	}
	_, _, err = ParseRedis(raw)
	return q, err
}

func redisInvocation(raw []byte) (operations.Kind, *operations.NativeSpec, error) {
	_, kind, err := ParseRedis(raw)
	if err != nil {
		return "", nil, err
	}
	return kind, &operations.NativeSpec{Provider: "redis", Command: "query", Parameters: []operations.Parameter{{Type: "json", Value: append(json.RawMessage(nil), raw...)}}, ReturnResult: kind == operations.NativeExecute}, nil
}
