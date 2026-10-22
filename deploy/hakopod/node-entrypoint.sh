#!/bin/sh
# Root bootstrap for the Kelvo node in a Hakopod pod (see deploy/hakopod/README.md).
# Checks Landlock and cgroup v2, builds the delegated cgroup subtree from
# docs/process-containment.md, then execs the node as 65532 with no capabilities.
# KELVO_ENTRYPOINT_DRY_RUN=1 prints the mutating commands instead of running them.
set -eu

UID_KELVO=65532
STATE_DIR=${KELVO_STATE_DIR:-/var/lib/kelvo}
DRY=${KELVO_ENTRYPOINT_DRY_RUN:-0}

die() { echo "kelvo-node-entrypoint: $*" >&2; exit 1; }
run() {
	if [ "$DRY" = 1 ]; then echo "+ $*"; else "$@"; fi
}
put() { # put VALUE FILE
	if [ "$DRY" = 1 ]; then echo "+ write '$1' > $2"; else printf '%s' "$1" > "$2"; fi
}

[ "$#" -gt 0 ] || set -- node --config /etc/kelvo/node.yml

if [ "$DRY" = 1 ]; then
	cg=${KELVO_CGROUP_ROOT:-/sys/fs/cgroup}
else
	[ "$(id -u)" = 0 ] || die "must start as root (uid $(id -u)); the pod needs a root security context"
	grep -qw landlock /sys/kernel/security/lsm 2>/dev/null || die "landlock is not an active LSM on this kernel"
	[ "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs ] || die "cgroup v2 is not mounted at /sys/fs/cgroup"
	# Our own cgroup is the delegated ancestor. Inside a private cgroup namespace
	# this is /sys/fs/cgroup itself; keep node.yml containment.root = <this>/jobs.
	cg=${KELVO_CGROUP_ROOT:-/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)}
	cg=${cg%/}
	[ -w "$cg/cgroup.procs" ] || die "cgroup $cg is not writable; is the container privileged?"
fi

echo "kelvo-node-entrypoint: cgroup root $cg (containment.root must be $cg/jobs)"

run mkdir -p "$cg/supervisor" "$cg/jobs"
put "$$" "$cg/supervisor/cgroup.procs"
put '+cpu +memory +pids' "$cg/cgroup.subtree_control"
# Delegate like systemd Delegate=yes: the whole subtree, including this cgroup's
# own cgroup.procs, so the unprivileged manager may place children into jobs/
# (moves and clone3 into a cgroup need write access at the common ancestor).
run chown -R "$UID_KELVO:$UID_KELVO" "$cg"

# Fail-closed egress for the node and its query children (uid 65532): only
# loopback, the cluster CIDRs in KELVO_ALLOW_CIDRS (NATS, gateway, db-api
# resolver) and the Tailscale interface the sidecar routes through. If the
# sidecar or the exit node is down, database connections fail instead of
# leaving from the server's own address. Set KELVO_EGRESS=open to skip (tests).
if [ "${KELVO_EGRESS:-closed}" != open ]; then
	[ -n "${KELVO_ALLOW_CIDRS:-}" ] || die "KELVO_ALLOW_CIDRS is required (comma-separated cluster CIDRs)"
	for t in iptables ip6tables; do
		run "$t" -N KELVO_EGRESS
		run "$t" -A KELVO_EGRESS -o lo -j ACCEPT
		run "$t" -A KELVO_EGRESS -o tailscale0 -j ACCEPT
	done
	set -f
	for cidr in $(printf '%s' "$KELVO_ALLOW_CIDRS" | tr ',' ' '); do
		case $cidr in
			*:*) run ip6tables -A KELVO_EGRESS -d "$cidr" -j ACCEPT ;;
			*) run iptables -A KELVO_EGRESS -d "$cidr" -j ACCEPT ;;
		esac
	done
	set +f
	for t in iptables ip6tables; do
		run "$t" -A KELVO_EGRESS -j REJECT
		run "$t" -I OUTPUT -m owner --uid-owner "$UID_KELVO" -j KELVO_EGRESS
	done
fi

run install -d -m 0700 -o "$UID_KELVO" -g "$UID_KELVO" "$STATE_DIR" "$STATE_DIR/scratch" "$STATE_DIR/containment"

if [ "$DRY" = 1 ]; then
	echo "+ exec setpriv --reuid=$UID_KELVO --regid=$UID_KELVO --clear-groups --inh-caps=-all --bounding-set=-all --no-new-privs /usr/local/bin/kelvo $*"
	exit 0
fi
exec setpriv --reuid="$UID_KELVO" --regid="$UID_KELVO" --clear-groups \
	--inh-caps=-all --bounding-set=-all --no-new-privs /usr/local/bin/kelvo "$@"
