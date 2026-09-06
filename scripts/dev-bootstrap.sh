#!/usr/bin/env bash
set -euo pipefail

security_dir=".local/security"
private_key="$security_dir/rule-control-private.pem"
public_key="$security_dir/rule-control-public.pem"
issuer="${CONTROL_JWT_ISSUER:-fraud-local}"
audience="${CONTROL_JWT_AUDIENCE:-fraud-control}"

base64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

create_token() {
  local subject="$1"
  local permissions="$2"
  local now expires header payload signing_input signature
  now="$(date -u +%s)"
  expires="$((now + 3600))"
  header="$(printf '%s' '{"alg":"RS256","typ":"JWT"}' | base64url)"
  payload="$(printf '{"iss":"%s","aud":"%s","sub":"%s","exp":%s,"permissions":[%s]}' \
    "$issuer" "$audience" "$subject" "$expires" "$permissions" | base64url)"
  signing_input="$header.$payload"
  signature="$(printf '%s' "$signing_input" | openssl dgst -sha256 -sign "$private_key" | base64url)"
  printf '%s.%s\n' "$signing_input" "$signature"
}

umask 077
mkdir -p "$security_dir"

if [[ ! -f "$private_key" ]]; then
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$private_key"
  openssl rsa -pubout -in "$private_key" -out "$public_key"
fi

chmod 600 "$private_key"
chmod 644 "$public_key"
create_token "rule-author" '"RULE_WRITE","RULE_READ"' >"$security_dir/rule-author.jwt"
create_token "rule-approver" '"RULE_APPROVE","RULE_READ"' >"$security_dir/rule-approver.jwt"
create_token "rule-auditor" '"AUDIT_READ"' >"$security_dir/rule-auditor.jwt"

printf 'Chave pública: %s\n' "$public_key"
printf 'Token de autor: %s\n' "$security_dir/rule-author.jwt"
printf 'Token de aprovador: %s\n' "$security_dir/rule-approver.jwt"
printf 'Token de auditor: %s\n' "$security_dir/rule-auditor.jwt"
