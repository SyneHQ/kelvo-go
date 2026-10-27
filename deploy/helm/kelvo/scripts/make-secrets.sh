#!/bin/sh
# Create the two secrets the chart references, with a private CA and random
# credentials. For evaluation clusters: certificates last 90 days and the CA key
# is kept in $DIR (default ./kelvo-pki, mode 0700). Prints no secret values.
# Usage: make-secrets.sh <release> <namespace> [tenant] [node ids...]
set -eu
release=${1:?release name}; ns=${2:?namespace}; tenant=${3:-default}
shift 2; [ $# -gt 0 ] && shift
[ $# -gt 0 ] || set -- n1
dir=${DIR:-./kelvo-pki}
umask 077; mkdir -p "$dir"; cd "$dir"
[ -f ca.key ] || openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 365 \
  -subj "/CN=Kelvo $release CA" -addext basicConstraints=critical,CA:true \
  -addext keyUsage=critical,keyCertSign,cRLSign -addext subjectKeyIdentifier=hash 2>/dev/null
leaf() { # name san
  printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n' "$2" > "$1.ext"
  openssl req -new -newkey rsa:2048 -nodes -keyout "$1.key" -out "$1.csr" -subj "/CN=$1" 2>/dev/null
  openssl x509 -req -in "$1.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -out "$1.crt" -days 90 -extfile "$1.ext" 2>/dev/null
  rm -f "$1.csr" "$1.ext"
}
leaf nats "DNS:$release-nats,DNS:$release-nats.$ns.svc"
leaf api "DNS:$release-gateway,DNS:$release-gateway.$ns.svc,DNS:localhost,IP:127.0.0.1"
leaf gateway-client "URI:spiffe://kelvo/gateway"
files="--from-file=ca.crt --from-file=nats.crt --from-file=nats.key --from-file=api.crt --from-file=api.key --from-file=gateway-client.crt --from-file=gateway-client.key"
for id in "$@"; do
  leaf "node-$id" "DNS:$release-node-$id,DNS:$release-node-$id.$ns.svc,URI:spiffe://kelvo/tenant/$tenant/worker/$id"
  files="$files --from-file=node-$id.crt --from-file=node-$id.key"
done
# NATS parses an interpolated $VAR as a config value: a password starting with
# a digit is read as a number and fails. Prefix a letter.
rand() { printf 'k%s' "$(openssl rand -hex 32)"; }
[ -f tenant-token ] || rand > tenant-token
# shellcheck disable=SC2086
kubectl -n "$ns" create secret generic "${TLS_SECRET:-kelvo-tls}" $files --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$ns" create secret generic "${SECRET:-kelvo-secrets}" \
  --from-file=tenant-token \
  --from-literal=nats-provisioner-password="$(rand)" \
  --from-literal=nats-gateway-password="$(rand)" \
  --from-literal=nats-worker-password="$(rand)" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "created secrets ${TLS_SECRET:-kelvo-tls} and ${SECRET:-kelvo-secrets} in $ns; tenant token in $dir/tenant-token, CA in $dir/ca.crt"
