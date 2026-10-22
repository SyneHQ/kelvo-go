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
run chown -R "$UID_KELVO:$UID_KELVO" "$cg/jobs"

run install -d -m 0700 -o "$UID_KELVO" -g "$UID_KELVO" "$STATE_DIR" "$STATE_DIR/scratch" "$STATE_DIR/containment"

if [ "$DRY" = 1 ]; then
	echo "+ exec setpriv --reuid=$UID_KELVO --regid=$UID_KELVO --clear-groups --inh-caps=-all --bounding-set=-all --no-new-privs /usr/local/bin/kelvo $*"
	exit 0
fi
exec setpriv --reuid="$UID_KELVO" --regid="$UID_KELVO" --clear-groups \
	--inh-caps=-all --bounding-set=-all --no-new-privs /usr/local/bin/kelvo "$@"
