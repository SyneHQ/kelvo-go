package provider

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestRedisInvocationPreservesArgumentsAndWriteAuthority(t *testing.T) {
	for _, test := range []struct {
		text string
		args []string
		kind operations.Kind
	}{
		{`GET "tenant key"`, []string{"GET", "tenant key"}, operations.NativeRead},
		{`SET name 'John Doe'`, []string{"SET", "name", "John Doe"}, operations.NativeExecute},
		{`SET name "value\nwith\x20space"`, []string{"SET", "name", "value\nwith space"}, operations.NativeExecute},
		{`SET name 'literal\ntext'`, []string{"SET", "name", `literal\ntext`}, operations.NativeExecute},
		{`SET empty ""`, []string{"SET", "empty", ""}, operations.NativeExecute},
	} {
		kind, spec, err := InvocationText("redis", test.text)
		if err != nil || kind != test.kind || spec == nil || spec.ReturnResult != kind.Mutating() {
			t.Fatal(test.text, kind, err)
		}
		q, parsed, err := ParseRedis(spec.Parameters[0].Value)
		if err != nil || parsed != kind || !reflect.DeepEqual(q.Args, test.args) {
			t.Fatal(test.text, q, err)
		}
	}
}

func TestRedisRejectsSessionCommandsScriptsAndAmbiguousBatches(t *testing.T) {
	for _, text := range []string{`SELECT 1`, `AUTH other secret`, `CONFIG GET *`, `EVAL "return 1" 0`, `SORT source STORE target`, `MULTI`, `EXEC`, `SUBSCRIBE other`, "GET x\nSET y z", `GET x; DEL y`, `SET x "unterminated`, `SET x "\xff"`, `GET foo\ bar`, `SET x "value"tail`} {
		if _, _, err := InvocationText("redis", text); err == nil {
			t.Fatal("unsafe command accepted", text)
		}
	}
	for _, command := range []string{"GETEX", "GETDEL", "XADD", "RENAME", "BITOP"} {
		raw, _ := json.Marshal(RedisQuery{Args: []string{command, "key"}})
		if _, kind, err := ParseRedis(raw); err != nil || kind != operations.NativeExecute {
			t.Fatal("write received read authority", command)
		}
	}
}
