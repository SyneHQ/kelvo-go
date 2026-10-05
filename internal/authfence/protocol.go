// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	streamInfoSubject    = "$JS.API.STREAM.INFO." + StreamName
	streamCreateSubject  = "$JS.API.STREAM.CREATE." + StreamName
	messageGetSubject    = "$JS.API.STREAM.MSG.GET." + StreamName
	expectStreamHeader   = "Nats-Expected-Stream"
	expectSequenceHeader = "Nats-Expected-Last-Subject-Sequence"
	expectSubjectHeader  = "Nats-Expected-Last-Subject-Sequence-Subject"
	streamInfoType       = "io.nats.jetstream.api.v1.stream_info_response"
	streamCreateType     = "io.nats.jetstream.api.v1.stream_create_response"
	messageGetType       = "io.nats.jetstream.api.v1.stream_msg_get_response"
)

type apiError struct {
	Code        int    `json:"code"`
	ErrCode     int    `json:"err_code"`
	Description string `json:"description"`
}

// Only pinned, definite outcomes are classified. Provider descriptions never
// leave this package and unknown server errors never prove nonpublication.
func classifyAPI(err *apiError) error {
	if err == nil {
		return nil
	}
	if err.Code == 403 {
		return ErrDenied
	}
	if err.Code == 400 && (err.ErrCode == 10071 || err.ErrCode == 10164 || err.ErrCode == 10058) {
		return ErrConflict
	}
	if err.Code == 404 && (err.ErrCode == 10037 || err.ErrCode == 10059) {
		return ErrNotFound
	}
	return ErrUnavailable
}

// These are the pinned plain-stream fields. Unsupported new fields fail
// closed rather than silently enabling a different storage protocol.
type streamConfig struct {
	Name                 string          `json:"name"`
	Description          string          `json:"description,omitempty"`
	Subjects             []string        `json:"subjects"`
	Retention            string          `json:"retention"`
	MaxConsumers         int             `json:"max_consumers"`
	MaxMsgs              int64           `json:"max_msgs"`
	MaxBytes             int64           `json:"max_bytes"`
	MaxAge               int64           `json:"max_age"`
	MaxMsgsPerSubject    int64           `json:"max_msgs_per_subject"`
	MaxMsgSize           int64           `json:"max_msg_size"`
	Discard              string          `json:"discard"`
	Storage              string          `json:"storage"`
	Replicas             int             `json:"num_replicas"`
	NoAck                bool            `json:"no_ack"`
	Duplicates           int64           `json:"duplicate_window"`
	Compression          string          `json:"compression"`
	FirstSeq             uint64          `json:"first_seq,omitempty"`
	Placement            json.RawMessage `json:"placement,omitempty"`
	Mirror               json.RawMessage `json:"mirror,omitempty"`
	Sources              json.RawMessage `json:"sources,omitempty"`
	SubjectTransform     json.RawMessage `json:"subject_transform,omitempty"`
	Republish            json.RawMessage `json:"republish,omitempty"`
	AllowDirect          bool            `json:"allow_direct"`
	MirrorDirect         bool            `json:"mirror_direct"`
	DiscardNewPerSubject bool            `json:"discard_new_per_subject"`
	Sealed               bool            `json:"sealed"`
	DenyDelete           bool            `json:"deny_delete"`
	DenyPurge            bool            `json:"deny_purge"`
	AllowRollup          bool            `json:"allow_rollup_hdrs"`
	ConsumerLimits       struct {
		InactiveThreshold int64 `json:"inactive_threshold,omitempty"`
		MaxAckPending     int64 `json:"max_ack_pending,omitempty"`
	} `json:"consumer_limits"`
	AllowMsgTTL            bool              `json:"allow_msg_ttl"`
	SubjectDeleteMarkerTTL int64             `json:"subject_delete_marker_ttl,omitempty"`
	AllowMsgCounter        bool              `json:"allow_msg_counter,omitempty"`
	AllowAtomic            bool              `json:"allow_atomic,omitempty"`
	AllowSchedules         bool              `json:"allow_msg_schedules,omitempty"`
	PersistMode            string            `json:"persist_mode,omitempty"`
	AllowBatched           bool              `json:"allow_batched,omitempty"`
	Metadata               map[string]string `json:"metadata,omitempty"`
}

func expectedConfig(scope Scope) streamConfig {
	subjects := []string{AuthoritySubject, "auth.witness." + ControlReplicaID}
	for _, id := range scope.Gateways() {
		subjects = append(subjects, "auth.witness."+id)
	}
	sort.Strings(subjects)
	return streamConfig{Name: StreamName, Subjects: subjects, Retention: "limits", MaxConsumers: -1,
		MaxMsgs: int64(2 * len(subjects)), MaxBytes: 2 << 20, MaxMsgsPerSubject: 1, MaxMsgSize: 32 << 10,
		Discard: "new", Storage: "file", Replicas: 3, Duplicates: int64(2 * time.Minute), Compression: "none",
		DenyDelete: true, DenyPurge: true, PersistMode: "default"}
}

func absent(value json.RawMessage) bool { return len(value) == 0 || bytes.Equal(value, []byte("null")) }

func validStreamConfig(raw []byte, scope Scope, fromServer bool) bool {
	var fields map[string]json.RawMessage
	if decodeJSON(raw, &fields, false) != nil {
		return false
	}
	for name, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			switch name {
			case "placement", "mirror", "sources", "subject_transform", "republish", "metadata":
			default:
				return false
			}
		}
	}
	if rawLimits, exists := fields["consumer_limits"]; exists {
		var limits map[string]json.RawMessage
		if decodeJSON(rawLimits, &limits, false) != nil {
			return false
		}
		for _, value := range limits {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return false
			}
		}
	}
	var got streamConfig
	if decodeJSON(raw, &got, true) != nil {
		return false
	}
	wanted := expectedConfig(scope)
	if got.PersistMode == "" {
		got.PersistMode = "default"
	}
	if len(got.Subjects) != len(wanted.Subjects) {
		return false
	}
	sort.Strings(got.Subjects)
	for i, subject := range wanted.Subjects {
		if got.Subjects[i] != subject {
			return false
		}
	}
	if !absent(got.Placement) || !absent(got.Mirror) || !absent(got.Sources) ||
		!absent(got.SubjectTransform) || !absent(got.Republish) {
		return false
	}
	if fromServer {
		// Pinned servers add only these versioning fields to plain streams.
		// Require the exact pair; never discard arbitrary operator metadata.
		if len(got.Metadata) != 3 || got.Metadata["_nats.req.level"] != "0" {
			return false
		}
		version, level := got.Metadata["_nats.ver"], got.Metadata["_nats.level"]
		if !((version == "2.14.7" && level == "4") || (version == "2.15.0" && level == "5")) {
			return false
		}
	} else if len(got.Metadata) != 0 {
		return false
	}
	got.Placement, got.Mirror, got.Sources, got.SubjectTransform, got.Republish = nil, nil, nil, nil, nil
	got.Metadata = nil
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(wanted)
	return bytes.Equal(a, b)
}

// Duplicate JSON fields are rejected at every nesting level. The response
// budget is checked before decoding; recursion and token count are bounded.
func decodeJSON(raw []byte, target any, strict bool) error {
	if len(raw) == 0 || len(raw) > maxResponseBytes {
		return ErrUnavailable
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	nodes := 0
	var value func(int) error
	value = func(depth int) error {
		nodes++
		if depth > 16 || nodes > 12000 {
			return ErrUnavailable
		}
		token, err := d.Token()
		if err != nil {
			return ErrUnavailable
		}
		delim, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				keyToken, err := d.Token()
				if err != nil {
					return ErrUnavailable
				}
				key, ok := keyToken.(string)
				if !ok {
					return ErrUnavailable
				}
				// encoding/json matches struct fields case-insensitively. Reject
				// aliases too, so differently cased duplicates cannot overwrite.
				key = strings.ToLower(key)
				if seen[key] {
					return ErrUnavailable
				}
				seen[key] = true
				if err = value(depth + 1); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return ErrUnavailable
			}
		case '[':
			for d.More() {
				if err = value(depth + 1); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return ErrUnavailable
			}
		default:
			return ErrUnavailable
		}
		return nil
	}
	if value(0) != nil {
		return ErrUnavailable
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrUnavailable
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	if strict {
		d.DisallowUnknownFields()
	}
	if d.Decode(target) != nil {
		return ErrUnavailable
	}
	return nil
}

func streamResponse(raw []byte, scope Scope, expectedType string) error {
	var response struct {
		Type   string          `json:"type"`
		Error  *apiError       `json:"error"`
		Config json.RawMessage `json:"config"`
		State  *struct {
			Consumers *int `json:"consumer_count"`
		} `json:"state"`
	}
	if decodeJSON(raw, &response, false) != nil {
		return ErrUnavailable
	}
	if response.Type != expectedType {
		return ErrUnavailable
	}
	if response.Error != nil && (!absent(response.Config) || response.State != nil) {
		return ErrUnavailable
	}
	if err := classifyAPI(response.Error); err != nil {
		return err
	}
	if response.State == nil || response.State.Consumers == nil || *response.State.Consumers != 0 ||
		!validStreamConfig(response.Config, scope, true) {
		return ErrUnavailable
	}
	return nil
}

func checkStream(op *operation, session wireSession, scope Scope) error {
	raw, err := op.request(session, streamInfoSubject, []byte("{}"), nil, false)
	if err != nil {
		return ErrUnavailable
	}
	return streamResponse(raw, scope, streamInfoType)
}

func readAuthority(op *operation, session wireSession, scope Scope) (Snapshot, error) {
	raw, err := op.request(session, messageGetSubject, []byte(`{"last_by_subj":"auth.current"}`), nil, false)
	if err != nil {
		return Snapshot{}, ErrUnavailable
	}
	var response struct {
		Type    string    `json:"type"`
		Error   *apiError `json:"error"`
		Message *struct {
			Subject  string `json:"subject"`
			Sequence uint64 `json:"seq"`
			Data     []byte `json:"data"`
		} `json:"message"`
	}
	if decodeJSON(raw, &response, false) != nil {
		return Snapshot{}, ErrUnavailable
	}
	if response.Type != messageGetType {
		return Snapshot{}, ErrUnavailable
	}
	if response.Error != nil && response.Message != nil {
		return Snapshot{}, ErrUnavailable
	}
	if err = classifyAPI(response.Error); err != nil {
		return Snapshot{}, err
	}
	if response.Message == nil ||
		response.Message.Subject != AuthoritySubject || response.Message.Sequence == 0 {
		return Snapshot{}, ErrUnavailable
	}
	record, err := DecodeRecord(response.Message.Data)
	if err != nil || !record.Scope().Equal(scope) {
		return Snapshot{}, ErrUnavailable
	}
	return Snapshot{record: record, authoritySequence: response.Message.Sequence}, nil
}

func parseAck(raw []byte, after uint64) (uint64, error) {
	var fields map[string]json.RawMessage
	if decodeJSON(raw, &fields, false) != nil {
		return 0, ErrUnknown
	}
	for key := range fields {
		switch key {
		case "stream", "seq", "duplicate", "domain", "error":
		default:
			return 0, ErrUnknown
		}
	}
	if value, exists := fields["duplicate"]; exists && !bytes.Equal(value, []byte("false")) && !bytes.Equal(value, []byte("true")) {
		return 0, ErrUnknown
	}
	if value, exists := fields["domain"]; exists && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return 0, ErrUnknown
	}
	for _, key := range []string{"error", "stream", "seq"} {
		if value, exists := fields[key]; exists && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return 0, ErrUnknown
		}
	}
	var response struct {
		Stream    string    `json:"stream"`
		Sequence  uint64    `json:"seq"`
		Duplicate bool      `json:"duplicate"`
		Domain    string    `json:"domain"`
		Error     *apiError `json:"error"`
	}
	if decodeJSON(raw, &response, true) != nil {
		return 0, ErrUnknown
	}
	if err := classifyAPI(response.Error); err != nil {
		// Pinned error ACKs may carry the stream with seq=0. A positive
		// sequence, duplicate or foreign stream mixed with an error is not
		// a definite failure and must never authorize a replay.
		if response.Sequence != 0 || (response.Stream != "" && response.Stream != StreamName) || response.Duplicate || response.Domain != "" {
			return 0, ErrUnknown
		}
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrDenied) {
			return 0, err
		}
		return 0, ErrUnknown
	}
	if response.Stream != StreamName || response.Sequence == 0 || response.Sequence <= after || response.Duplicate || response.Domain != "" {
		return 0, ErrUnknown
	}
	return response.Sequence, nil
}

func guardedPublish(op *operation, session wireSession, subject string, data []byte, after uint64) (uint64, error) {
	header := nats.Header{}
	header.Set(expectStreamHeader, StreamName)
	header.Set(expectSequenceHeader, strconv.FormatUint(after, 10))
	header.Set(expectSubjectHeader, AuthoritySubject)
	raw, err := op.request(session, subject, data, header, true)
	if err != nil {
		return 0, ErrUnknown
	}
	sequence, err := parseAck(raw, after)
	if !op.fresh() {
		return 0, ErrUnknown
	}
	return sequence, err
}

func verifyAuthority(op *operation, session wireSession, config Config, expected Document) (VerifiedObservation, error) {
	if err := checkStream(op, session, config.Scope); err != nil {
		return VerifiedObservation{}, err
	}
	snapshot, err := readAuthority(op, session, config.Scope)
	if err != nil {
		return VerifiedObservation{}, err
	}
	if !snapshot.Record().Document().Equal(expected) {
		return VerifiedObservation{}, ErrConflict
	}
	var nonce [24]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return VerifiedObservation{}, ErrUnavailable
	}
	payload, _ := json.Marshal(struct {
		Version   int    `json:"version"`
		Nonce     string `json:"nonce"`
		Authority uint64 `json:"authority_sequence"`
	}{Version, hex.EncodeToString(nonce[:]), snapshot.AuthoritySequence()})
	if len(payload) > MaxWitnessBytes {
		return VerifiedObservation{}, ErrInvalid
	}
	sequence, err := guardedPublish(op, session, "auth.witness."+config.ReplicaID, payload, snapshot.AuthoritySequence())
	if err != nil {
		return VerifiedObservation{}, err
	}
	return VerifiedObservation{snapshot: snapshot, witnessSequence: sequence, attempt: op.attempt, verified: true}, nil
}

func mutationFailure(err error) (MutationResult, error) {
	switch {
	case errors.Is(err, ErrConflict):
		return MutationResult{outcome: OutcomeConflict}, ErrConflict
	case errors.Is(err, ErrDenied):
		return MutationResult{outcome: OutcomeDenied}, ErrDenied
	case errors.Is(err, ErrInvalid):
		return MutationResult{outcome: OutcomeInvalid}, ErrInvalid
	case errors.Is(err, ErrUnknown):
		return MutationResult{outcome: OutcomeUnknown}, ErrUnknown
	default:
		return MutationResult{}, publicError(err)
	}
}

func mutateAuthority(op *operation, session wireSession, config Config, document Document, revision uint64, initialize bool) (MutationResult, error) {
	err := checkStream(op, session, config.Scope)
	if errors.Is(err, ErrNotFound) && initialize {
		payload, _ := json.Marshal(expectedConfig(config.Scope))
		raw, sendErr := op.request(session, streamCreateSubject, payload, nil, true)
		if sendErr != nil {
			return mutationFailure(ErrUnknown)
		}
		if err = streamResponse(raw, config.Scope, streamCreateType); err != nil {
			if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrDenied) {
				err = ErrUnknown
			}
			return mutationFailure(err)
		}
	} else if err != nil {
		return mutationFailure(err)
	}
	current, err := readAuthority(op, session, config.Scope)
	var after uint64
	if initialize {
		if err == nil {
			return mutationFailure(ErrConflict)
		}
		if !errors.Is(err, ErrNotFound) {
			return mutationFailure(err)
		}
	} else {
		if err != nil {
			return mutationFailure(err)
		}
		if current.Record().Document().Revision() != revision || document.Revision() <= revision {
			return mutationFailure(ErrConflict)
		}
		after = current.AuthoritySequence()
	}
	record, err := NewRecord(config.Scope, document)
	if err != nil {
		return mutationFailure(ErrInvalid)
	}
	payload, err := EncodeRecord(record)
	if err != nil {
		return mutationFailure(ErrInvalid)
	}
	sequence, err := guardedPublish(op, session, AuthoritySubject, payload, after)
	if err != nil {
		return mutationFailure(err)
	}
	outcome := OutcomeAdvanced
	if initialize {
		outcome = OutcomeCreated
	}
	return MutationResult{outcome: outcome, snapshot: Snapshot{record: record, authoritySequence: sequence}}, nil
}
