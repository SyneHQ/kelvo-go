#!/bin/sh
# Dry-run test for node-entrypoint.sh; needs no kernel support.
set -eu
d=$(dirname "$0")
out=$(KELVO_ENTRYPOINT_DRY_RUN=1 KELVO_CGROUP_ROOT=/cg KELVO_STATE_DIR=/st sh "$d/node-entrypoint.sh")
has() { printf '%s\n' "$out" | grep -qxF -- "$1" || { echo "FAIL missing: $1"; echo "$out"; exit 1; }; }
has '+ mkdir -p /cg/supervisor /cg/jobs'
has "+ write '+cpu +memory +pids' > /cg/cgroup.subtree_control"
has '+ chown -R 65532:65532 /cg/jobs'
has '+ install -d -m 0700 -o 65532 -g 65532 /st /st/scratch /st/containment'
has '+ exec setpriv --reuid=65532 --regid=65532 --clear-groups --inh-caps=-all --bounding-set=-all --no-new-privs /usr/local/bin/kelvo node --config /etc/kelvo/node.yml'
if printf '%s\n' "$out" | grep -qE 'tailscale|iptables'; then echo "FAIL: network rules do not belong here"; exit 1; fi
# explicit args are passed through
KELVO_ENTRYPOINT_DRY_RUN=1 KELVO_CGROUP_ROOT=/cg sh "$d/node-entrypoint.sh" node --check-config | grep -q 'kelvo node --check-config$' || { echo "FAIL args"; exit 1; }
# a non-root real run must refuse
if [ "$(id -u)" != 0 ]; then
	if sh "$d/node-entrypoint.sh" 2>/dev/null; then echo "FAIL: ran as non-root"; exit 1; fi
fi
echo OK
