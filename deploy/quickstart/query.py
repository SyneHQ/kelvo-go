"""Query the quickstart server and print the Arrow result.

Usage: KELVO_TOKEN=... python3 query.py ["SQL"]   (needs: pip install pyarrow)
"""
import json
import os
import sys
import urllib.request

import pyarrow.ipc

BASE = os.environ.get("KELVO_URL", "http://127.0.0.1:8080")
SQL = sys.argv[1] if len(sys.argv) > 1 else (
    "SELECT region, SUM(amount::DECIMAL(18,2)) AS revenue FROM sales GROUP BY region ORDER BY region")
HEADERS = {"Authorization": "Bearer " + os.environ["KELVO_TOKEN"], "Content-Type": "application/json"}


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    return urllib.request.urlopen(urllib.request.Request(BASE + path, data, HEADERS, method=method))


handle = json.load(call("POST", "/v1/queries", {"mode": "federated", "sources": ["sales"], "sql": SQL}))
# Results are claimed once; read the whole stream, then confirm terminal status.
table = pyarrow.ipc.open_stream(call("GET", f"/v1/queries/{handle['id']}/results").read()).read_all()
status = json.load(call("GET", f"/v1/queries/{handle['id']}"))
if status["state"] != "succeeded":
    raise SystemExit(f"query {status['state']}: {status.get('error')}")
print(table)
