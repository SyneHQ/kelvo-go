"""Allowlisted public diagnostics from private export lifecycle test output."""
import json
import re

PACKAGE = "github.com/SYNEHQ/kelvo-go/internal/cluster"
TEST = "TestExportClusterActualWorkerLifecycle"
TESTS = {TEST} | {TEST + "/" + name for name in (
    "accepted-export-survives-submit-disconnect", "lz4-preserves-values-and-null-policy",
    "ready-export-survives-worker-restart", "lost-completion-is-not-replayed",
    "fresh-key-rechecks-ready-downloads")}
MARKER_TAG = "KELVO_EXPORT_WAIT_STATE"
MARKER = MARKER_TAG + " "
SOURCE = "export_integration_linux_test.go"
LOCATION = re.compile(r"^[ \t]*export_integration_linux_test\.go:([1-9][0-9]{0,5}):[ \t]*(.*)$")
STATES = {"queued", "assigned", "claimed", "running", "stored", "ready", "failed",
          "cancelled", "publication_uncertain"}
ERROR_CLASSES = {"none", "context_cancelled", "deadline_exceeded", "not_found", "conflict",
                 "store_error", "job_cancelled", "job_deadline_exceeded", "job_unavailable", "job_error"}
FIELDS = {"wanted", "observed", "deadline_reached", "receipt_present", "error_class", "executions"}
MAX_PAYLOAD = 4096
MAX_OUTPUT = 8192
MAX_RECORDS = 32
MAX_COUNTER = 65535


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate diagnostic field")
        value[key] = item
    return value


def parse_wait_observation(payload):
    """Reject every field or representation outside the closed public schema."""
    if not isinstance(payload, str) or len(payload) > MAX_PAYLOAD or not payload.isascii():
        raise ValueError("invalid diagnostic payload")
    value = json.loads(payload, object_pairs_hook=_unique_object)
    if type(value) is not dict or set(value) != FIELDS:
        raise ValueError("invalid diagnostic fields")
    if type(value["wanted"]) is not str or value["wanted"] not in STATES:
        raise ValueError("invalid wanted state")
    if type(value["observed"]) is not str or value["observed"] not in STATES | {"unknown"}:
        raise ValueError("invalid observed state")
    if type(value["error_class"]) is not str or value["error_class"] not in ERROR_CLASSES:
        raise ValueError("invalid error class")
    if type(value["deadline_reached"]) is not bool or type(value["receipt_present"]) is not bool:
        raise ValueError("invalid diagnostic boolean")
    if type(value["executions"]) is not int or not 0 <= value["executions"] <= 2**31 - 1:
        raise ValueError("invalid execution count")
    return value


class ExportDiagnostics:
    """Bound retained records and counts; never copy free text into a receipt."""
    def __init__(self):
        self.records = []
        self.rejected = 0
        self.truncated = 0

    def reject(self):
        self.rejected = min(MAX_COUNTER, self.rejected + 1)

    def retain(self, record):
        if len(self.records) == MAX_RECORDS:
            self.truncated = min(MAX_COUNTER, self.truncated + 1)
        else:
            self.records.append(record)

    def observe(self, event):
        if type(event) is not dict or event.get("Action") != "output":
            return
        output = event.get("Output")
        if type(output) is not str:
            self.reject()
            return
        if len(output) > MAX_OUTPUT:
            self.reject()
            return
        candidate = MARKER_TAG in output or SOURCE + ":" in output
        if event.get("Package") != PACKAGE or type(event.get("Test")) is not str or event["Test"] not in TESTS:
            if candidate:
                self.reject()
            return
        for line in output.splitlines():
            location = LOCATION.fullmatch(line)
            if location is None:
                if MARKER_TAG in line or SOURCE + ":" in line:
                    self.reject()
                continue
            line_number, text = location.groups()
            record = {"test": event["Test"], "file": SOURCE, "line": int(line_number)}
            if text.startswith(MARKER):
                try:
                    observation = parse_wait_observation(text[len(MARKER):])
                except (ValueError, TypeError, RecursionError):
                    self.reject()
                    continue
                record.update(kind="wait_state", **observation)
            elif MARKER_TAG in text:
                self.reject()
                continue
            else:
                record["kind"] = "source_location"
            self.retain(record)

    def report(self):
        return {"records": list(self.records), "rejected": self.rejected, "truncated": self.truncated}
