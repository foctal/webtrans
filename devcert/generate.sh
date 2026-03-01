#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

CERT_NAME="localhost"
DAYS=10
CONF="openssl.conf"

echo "==> Generating self-signed WebTransport certificate (${CERT_NAME})"
echo "    validity: ${DAYS} days"
echo "    config:   ${CONF}"

# Generate ECDSA P-256 private key
openssl ecparam \
  -genkey \
  -name prime256v1 \
  -out "${CERT_NAME}.key"

# Generate self-signed certificate
openssl req \
  -x509 \
  -sha256 \
  -nodes \
  -days "${DAYS}" \
  -key "${CERT_NAME}.key" \
  -out "${CERT_NAME}.crt" \
  -config "${CONF}" \
  -extensions v3_req

# Generate raw SHA-256 hash (DER -> hex, no colons)
openssl x509 \
  -in "${CERT_NAME}.crt" \
  -outform der \
| openssl dgst -sha256 -binary \
| xxd -p -c 256 \
> "${CERT_NAME}.hex"

# Also print human-readable fingerprint
openssl x509 \
  -in "${CERT_NAME}.crt" \
  -noout \
  -fingerprint -sha256 \
> "${CERT_NAME}.fingerprint"

echo "==> Done"
echo "  - ${CERT_NAME}.crt"
echo "  - ${CERT_NAME}.key"
echo "  - ${CERT_NAME}.hex      (for serverCertificateHashes)"
echo "  - ${CERT_NAME}.fingerprint"
