#!/bin/sh
# Create every secret the `kelvo` Hakopod application references, without any
# value leaving this machine except to Hakopod. Run it yourself, after
# `hakopod login`. Nothing secret is printed.
#
#   sh deploy/hakopod/create-secrets.sh [PROJECT] [ENVIRONMENT] [APPLICATION]
#
# It generates a private CA (kept in $KELVO_CA_DIR, default ~/.kelvo-hakopod-ca,
# mode 0700: back it up, it signs future node and gateway certificates), the
# NATS, gateway and node certificates from deploy/hakopod/README.md section 3,
# the API token and NATS passwords. It asks for the test database DSN with a
# hidden prompt. Tailscale needs no secret: the sidecar logs in interactively.
#
# HAKOPOD (default: hakopod) can point at another CLI binary. KELVO_SECRETS_DRY_RUN=1
# prints the secret names it would store instead of calling Hakopod.
set -eu
umask 077

PROJECT=${1:-demo}
ENVIRONMENT=${2:-production}
APPLICATION=${3:-kelvo}
TENANT=kelvo-test
WORKER=n1
HAKOPOD=${HAKOPOD:-hakopod}
CA_DIR=${KELVO_CA_DIR:-$HOME/.kelvo-hakopod-ca}
DRY=${KELVO_SECRETS_DRY_RUN:-0}

die() { echo "create-secrets: $*" >&2; exit 1; }
for tool in openssl python3; do command -v "$tool" >/dev/null || die "$tool is required"; done
if [ "$DRY" != 1 ]; then
	command -v "$HAKOPOD" >/dev/null || die "the hakopod CLI is required"
	"$HAKOPOD" projects >/dev/null 2>&1 || die "run 'hakopod login --api-url https://console.synehq.com:8443 --project $PROJECT --environment $ENVIRONMENT' first"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM

# put NAME FILE: store FILE's content as the application secret NAME.
put() {
	if [ "$DRY" = 1 ]; then echo "would store $1"; return; fi
	python3 -c 'import json,sys; json.dump({"value": open(sys.argv[1]).read()}, open(sys.argv[2], "w"))' "$2" "$work/body.json"
	"$HAKOPOD" api call putWorkloadSecret \
		--path-json "{\"name\":\"$1\"}" \
		--query-json "{\"project\":\"$PROJECT\",\"environment\":\"$ENVIRONMENT\",\"application\":\"$APPLICATION\"}" \
		--body-file "$work/body.json" --allow-write >/dev/null || die "storing $1 failed"
	rm -f "$work/body.json"
	echo "stored $1"
}

# KELVO_ONLY_PASSWORDS=1 rotates only the API token and NATS passwords.
ONLY_PASSWORDS=${KELVO_ONLY_PASSWORDS:-0}

if [ "$ONLY_PASSWORDS" != 1 ]; then
# CA: reuse an existing one so re-running rotates leaves, not trust.
mkdir -p "$CA_DIR"
chmod 0700 "$CA_DIR"
if [ ! -f "$CA_DIR/ca.key" ]; then
	openssl req -x509 -new -newkey rsa:3072 -nodes -keyout "$CA_DIR/ca.key" -out "$CA_DIR/ca.pem" \
		-days 825 -subj "/CN=kelvo-$TENANT-ca" \
		-addext "basicConstraints=critical,CA:true" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
	echo "created CA in $CA_DIR (back it up)"
fi

# leaf NAME CN SAN: a 90-day server and client certificate signed by the CA.
leaf() {
	openssl req -new -newkey rsa:2048 -nodes -keyout "$work/$1.key" -out "$work/$1.csr" -subj "/CN=$2" 2>/dev/null
	printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\n' "$3" > "$work/$1.ext"
	openssl x509 -req -in "$work/$1.csr" -CA "$CA_DIR/ca.pem" -CAkey "$CA_DIR/ca.key" -CAcreateserial \
		-out "$work/$1.crt" -days 90 -extfile "$work/$1.ext" 2>/dev/null
}

leaf nats kelvo-nats "DNS:nats"
leaf api kelvo-gateway "DNS:gateway"
leaf gateway-client kelvo-gateway-client "URI:spiffe://kelvo/gateway"
leaf node kelvo-node "DNS:node,URI:spiffe://kelvo/tenant/$TENANT/worker/$WORKER"

put kelvo-ca-crt "$CA_DIR/ca.pem"
put kelvo-nats-crt "$work/nats.crt"
put kelvo-nats-key "$work/nats.key"
put kelvo-api-crt "$work/api.crt"
put kelvo-api-key "$work/api.key"
put kelvo-gateway-client-crt "$work/gateway-client.crt"
put kelvo-gateway-client-key "$work/gateway-client.key"
put kelvo-node-crt "$work/node.crt"
put kelvo-node-key "$work/node.key"
fi

# NATS parses an environment variable's value as config syntax, so a value that
# starts with a digit is read as a number and fails. Start every value with a letter.
for name in kelvo-api-token kelvo-nats-provisioner-password kelvo-nats-gateway-password kelvo-nats-worker-password; do
	{ printf k; openssl rand -hex 32 | tr -d '\n'; } > "$work/$name"
	put "$name" "$work/$name"
done

# prompt NAME LABEL: read a value without echo and store it.
prompt() {
	if [ "$DRY" = 1 ]; then echo "would ask for $1"; return; fi
	printf '%s: ' "$2" >&2
	stty -echo 2>/dev/null || true
	IFS= read -r value || true
	stty echo 2>/dev/null || true
	echo >&2
	[ -n "$value" ] || die "$1 is required"
	printf '%s' "$value" > "$work/$1"
	value=
	put "$1" "$work/$1"
}

if [ "$ONLY_PASSWORDS" = 1 ]; then echo "done: API token and NATS passwords rotated"; exit 0; fi
prompt kelvo-source-postgres-dsn "Test database DSN (read-only user)"

echo "done: 14 secrets stored for $PROJECT/$ENVIRONMENT/$APPLICATION"
