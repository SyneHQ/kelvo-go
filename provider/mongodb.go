package provider

import (
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func mongoCollectionName(name string) bool {
	return name != "" && len(name) <= 120 && utf8.ValidString(name) && !strings.ContainsAny(name, "$\x00\r\n") && !strings.HasPrefix(name, "system.")
}

// MongoInvocationText recognizes JSON arguments, never JavaScript or mongosh.
// Extended JSON carries BSON-specific values such as ObjectID and Decimal128.
func MongoInvocationText(input string) (operations.Kind, *operations.NativeSpec, error) {
	deny := func() (operations.Kind, *operations.NativeSpec, error) { return "", nil, operations.ErrUnsupported }
	input = strings.TrimSpace(input)
	if len(input) > 16<<10 {
		return deny()
	}
	var fields map[string]json.RawMessage
	command := ""
	if strings.HasPrefix(input, "{") {
		if DecodeDocument([]byte(input), &fields, 16<<10) != nil || fields == nil {
			return deny()
		}
		if raw, ok := fields["command"]; ok {
			if json.Unmarshal(raw, &command) != nil {
				return deny()
			}
			delete(fields, "command")
		} else if _, ok := fields["pipeline"]; ok {
			command = "aggregate"
		}
	} else {
		var err error
		command, fields, err = parseMongoShell(input)
		if err != nil {
			return deny()
		}
	}
	var collection string
	if json.Unmarshal(fields["collection"], &collection) != nil || !mongoCollectionName(collection) {
		return deny()
	}
	var kind operations.Kind
	switch command {
	case "find", "find_one", "aggregate", "count", "list_indexes":
		kind = operations.NativeRead
	case "insert_one", "insert_many", "update_one", "update_many", "delete_one", "delete_many", "create_collection", "drop_collection", "create_index", "drop_index":
		kind = operations.NativeExecute
	default:
		return deny()
	}
	raw, err := json.Marshal(fields)
	if err != nil || len(raw) > 16<<10 {
		return deny()
	}
	spec := &operations.NativeSpec{Provider: "mongodb", Command: command, Parameters: []operations.Parameter{{Type: "json", Value: raw}}}
	spec.ReturnResult = kind == operations.NativeExecute
	return kind, spec, nil
}

func mongoCall(input string) (string, []json.RawMessage, string, error) {
	open := strings.IndexByte(input, '(')
	if open < 1 {
		return "", nil, "", operations.ErrInvalid
	}
	quoted, escaped := false, false
	end := -1
	for i := open + 1; i < len(input); i++ {
		c := input[i]
		if quoted {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
			continue
		}
		if c == '(' {
			return "", nil, "", operations.ErrInvalid
		}
		if c == ')' {
			end = i
			break
		}
	}
	if end < 0 {
		return "", nil, "", operations.ErrInvalid
	}
	var args []json.RawMessage
	if DecodeDocument([]byte("["+input[open+1:end]+"]"), &args, 16<<10) != nil {
		return "", nil, "", operations.ErrInvalid
	}
	return input[:open], args, strings.TrimSpace(input[end+1:]), nil
}

func parseMongoShell(input string) (string, map[string]json.RawMessage, error) {
	bad := func() (string, map[string]json.RawMessage, error) { return "", nil, operations.ErrUnsupported }
	call, args, tail, err := mongoCall(strings.TrimSuffix(input, ";"))
	if err != nil {
		return bad()
	}
	parts := strings.Split(call, ".")
	fields := map[string]json.RawMessage{}
	method, collection := "", ""
	if len(parts) == 2 && parts[0] == "db" && parts[1] == "createCollection" && len(args) == 1 {
		if json.Unmarshal(args[0], &collection) != nil {
			return bad()
		}
		method = "createCollection"
		args = nil
	} else if len(parts) == 3 && parts[0] == "db" {
		collection, method = parts[1], parts[2]
	} else {
		return bad()
	}
	if !mongoCollectionName(collection) {
		return bad()
	}
	fields["collection"], _ = json.Marshal(collection)
	command := map[string]string{"find": "find", "findOne": "find_one", "aggregate": "aggregate", "countDocuments": "count", "getIndexes": "list_indexes", "insertOne": "insert_one", "insertMany": "insert_many", "updateOne": "update_one", "updateMany": "update_many", "deleteOne": "delete_one", "deleteMany": "delete_many", "createCollection": "create_collection", "drop": "drop_collection", "createIndex": "create_index", "dropIndex": "drop_index"}[method]
	if command == "" {
		return bad()
	}
	switch command {
	case "find", "find_one":
		if len(args) > 2 {
			return bad()
		}
		if len(args) > 0 {
			fields["filter"] = args[0]
		}
		if len(args) > 1 {
			fields["projection"] = args[1]
		}
	case "count":
		if len(args) > 1 {
			return bad()
		}
		if len(args) == 1 {
			fields["filter"] = args[0]
		}
	case "aggregate", "insert_one", "insert_many", "drop_index":
		if len(args) != 1 {
			return bad()
		}
		key := map[string]string{"aggregate": "pipeline", "insert_one": "document", "insert_many": "documents", "drop_index": "name"}[command]
		fields[key] = args[0]
	case "update_one", "update_many":
		if len(args) < 2 || len(args) > 3 {
			return bad()
		}
		fields["filter"], fields["update"] = args[0], args[1]
		if len(args) == 3 {
			var options map[string]json.RawMessage
			if DecodeDocument(args[2], &options, 16<<10) != nil || len(options) != 1 || options["upsert"] == nil {
				return bad()
			}
			fields["upsert"] = options["upsert"]
		}
	case "delete_one", "delete_many":
		if len(args) != 1 {
			return bad()
		}
		fields["filter"] = args[0]
	case "create_index":
		if len(args) != 2 {
			return bad()
		}
		fields["keys"] = args[0]
		var options map[string]json.RawMessage
		if DecodeDocument(args[1], &options, 16<<10) != nil || options["name"] == nil {
			return bad()
		}
		for key, value := range options {
			if key != "name" && key != "unique" {
				return bad()
			}
			fields[key] = value
		}
	default:
		if len(args) != 0 {
			return bad()
		}
	}
	if tail == "" {
		return command, fields, nil
	}
	if command != "find" {
		return bad()
	}
	pipeline := []json.RawMessage{}
	appendStage := func(key string, value json.RawMessage) {
		b, _ := json.Marshal(map[string]json.RawMessage{key: value})
		pipeline = append(pipeline, b)
	}
	if filter := fields["filter"]; filter != nil {
		appendStage("$match", filter)
	}
	seen := map[string]bool{}
	for tail != "" {
		name, values, rest, e := mongoCall(tail)
		if e != nil || len(values) != 1 || seen[name] {
			return bad()
		}
		seen[name] = true
		switch name {
		case ".sort":
			if seen[".limit"] || seen[".skip"] {
				return bad()
			}
			appendStage("$sort", values[0])
		case ".skip", ".limit":
			n, e := strconv.ParseInt(string(values[0]), 10, 32)
			if e != nil || n < 0 || n > 1000000 || name == ".limit" && n == 0 || name == ".skip" && seen[".limit"] {
				return bad()
			}
			appendStage("$"+name[1:], values[0])
		default:
			return bad()
		}
		tail = rest
	}
	if projection := fields["projection"]; projection != nil {
		appendStage("$project", projection)
	}
	fields = map[string]json.RawMessage{"collection": fields["collection"]}
	fields["pipeline"], _ = json.Marshal(pipeline)
	return "aggregate", fields, nil
}
