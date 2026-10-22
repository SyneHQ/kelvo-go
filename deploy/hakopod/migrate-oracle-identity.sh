#!/bin/sh
# Copy the production Kelvo identity from the retired Oracle install into
# Hakopod secrets, so db-api keeps its service token, delegation key and
# resolver trust. Run it yourself after `hakopod login`; values go from the
# Oracle VM straight into Hakopod and are never printed.
#
#   sh deploy/hakopod/migrate-oracle-identity.sh [PROJECT] [ENVIRONMENT] [APPLICATION]
#
# ORACLE (default ubuntu@140.245.31.20) and ORACLE_KEY (default
# ~/.ssh/dokku-syne) select the SSH login.
set -eu
umask 077
PROJECT=${1:-demo}
ENVIRONMENT=${2:-production}
APPLICATION=${3:-kelvo}
ORACLE=${ORACLE:-ubuntu@140.245.31.20}
ORACLE_KEY=${ORACLE_KEY:-$HOME/.ssh/dokku-syne}
HAKOPOD=${HAKOPOD:-hakopod}

die() { echo "migrate-oracle-identity: $*" >&2; exit 1; }
command -v python3 >/dev/null || die "python3 is required"
"$HAKOPOD" projects >/dev/null 2>&1 || die "run 'hakopod login' first"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM

# copy SECRET ORACLE_PATH
copy() {
	ssh -o BatchMode=yes -i "$ORACLE_KEY" "$ORACLE" "sudo cat $2" > "$work/value" || die "reading $2 failed"
	[ -s "$work/value" ] || die "$2 is empty"
	python3 -c 'import json,sys; json.dump({"value": open(sys.argv[1]).read()}, open(sys.argv[2], "w"))' "$work/value" "$work/body.json"
	"$HAKOPOD" api call putWorkloadSecret \
		--path-json "{\"name\":\"$1\"}" \
		--query-json "{\"project\":\"$PROJECT\",\"environment\":\"$ENVIRONMENT\",\"application\":\"$APPLICATION\"}" \
		--body-file "$work/body.json" --allow-write >/dev/null || die "storing $1 failed"
	rm -f "$work/value" "$work/body.json"
	echo "stored $1"
}

copy kelvo-prod-service-keys /etc/kelvo/gateway/service-keys.yml
copy kelvo-prod-internal-ca /etc/kelvo/gateway/internal-ca.pem
copy kelvo-prod-gateway-internal-crt /etc/kelvo/gateway/internal.pem
copy kelvo-prod-gateway-internal-key /etc/kelvo/gateway/internal.key
copy kelvo-prod-node-internal-crt /etc/kelvo/node/internal.pem
copy kelvo-prod-node-internal-key /etc/kelvo/node/internal.key
copy kelvo-prod-resolver-ca /etc/kelvo/node/resolver-ca.pem
copy kelvo-prod-rabbit-ca /etc/kelvo/node/rabbit-ca.pem
echo "done: production identity copied to $PROJECT/$ENVIRONMENT/$APPLICATION"
