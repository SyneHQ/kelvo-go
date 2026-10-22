#!/bin/sh
# Dry-run test for node-entrypoint.sh; needs no kernel support.
set -eu
d=$(dirname "$0")
out=$(KELVO_ENTRYPOINT_DRY_RUN=1 KELVO_CGROUP_ROOT=/cg KELVO_STATE_DIR=/st KELVO_ALLOW_CIDRS=10.42.0.0/16,10.43.0.0/16,fd00::/8 sh "$d/node-entrypoint.sh")
has() { printf '%s\n' "$out" | grep -qxF -- "$1" || { echo "FAIL missing: $1"; echo "$out"; exit 1; }; }
has '+ mkdir -p /cg/supervisor /cg/jobs'
has "+ write '+cpu +memory +pids' > /cg/cgroup.subtree_control"
has '+ chown -R 65532:65532 /cg'
has '+ install -d -m 0700 -o 65532 -g 65532 /st /st/scratch /st/containment'
has '+ exec setpriv --reuid=65532 --regid=65532 --clear-groups --inh-caps=-all --bounding-set=-all --no-new-privs /usr/local/bin/kelvo node --config /etc/kelvo/node.yml'
has '+ iptables -A KELVO_EGRESS -o tailscale0 -j ACCEPT'
has '+ iptables -A KELVO_EGRESS -d 10.42.0.0/16 -j ACCEPT'
has '+ iptables -A KELVO_EGRESS -d 10.43.0.0/16 -j ACCEPT'
has '+ ip6tables -A KELVO_EGRESS -d fd00::/8 -j ACCEPT'
has '+ iptables -A KELVO_EGRESS -j REJECT'
has '+ ip6tables -A KELVO_EGRESS -j REJECT'
has '+ iptables -I OUTPUT -m owner --uid-owner 65532 -j KELVO_EGRESS'
has '+ ip6tables -I OUTPUT -m owner --uid-owner 65532 -j KELVO_EGRESS'
# the rules come before privileges are dropped
printf '%s\n' "$out" | awk '/OUTPUT -m owner/{r=NR} /exec setpriv/{e=NR} END{exit !(r && e && r<e)}' || { echo "FAIL: egress rules after exec"; exit 1; }
# missing CIDRs is refused unless egress is explicitly open
if KELVO_ENTRYPOINT_DRY_RUN=1 KELVO_CGROUP_ROOT=/cg sh "$d/node-entrypoint.sh" >/dev/null 2>&1; then echo "FAIL: ran without KELVO_ALLOW_CIDRS"; exit 1; fi
# explicit args are passed through
KELVO_ENTRYPOINT_DRY_RUN=1 KELVO_EGRESS=open KELVO_CGROUP_ROOT=/cg sh "$d/node-entrypoint.sh" node --check-config | grep -q 'kelvo node --check-config$' || { echo "FAIL args"; exit 1; }
# a non-root real run must refuse
if [ "$(id -u)" != 0 ]; then
	if sh "$d/node-entrypoint.sh" 2>/dev/null; then echo "FAIL: ran as non-root"; exit 1; fi
fi
echo OK
