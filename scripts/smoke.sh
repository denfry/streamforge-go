#!/usr/bin/env bash
set -euo pipefail

base_url="${STREAMFORGE_BASE_URL:-http://localhost:8080}"
campaign_json="$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d '{"name":"Smoke campaign","daily_budget":100,"currency":"USD"}' \
  "${base_url}/v1/campaigns")"
campaign_id="$(python -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"${campaign_json}")"

curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"event_id\":\"4a4b3b5d-40ca-4d43-b3c4-2ed4bd4bced8\",\"campaign_id\":\"${campaign_id}\",\"user_id\":\"smoke-user\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
  "${base_url}/v1/events/impression" >/dev/null

curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"event_id\":\"5b5c4c6e-51da-4e44-c4d5-3fe5ce5cdf90\",\"campaign_id\":\"${campaign_id}\",\"user_id\":\"smoke-user\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
  "${base_url}/v1/events/click" >/dev/null

for _ in {1..40}; do
  stats="$(curl --fail --silent --show-error "${base_url}/v1/stats/campaign/${campaign_id}")"
  if python -c 'import json,sys; d=json.load(sys.stdin); raise SystemExit(0 if d["impressions"] == 1 and d["clicks"] == 1 else 1)' <<<"${stats}"; then
    python -c 'import json,sys; raise SystemExit(0 if json.load(sys.stdin)["status"] == "ok" else 1)' <<<"$(curl --fail --silent --show-error "${base_url}/health")"
    metrics="$(curl --fail --silent --show-error "${base_url}/metrics")"
    test -n "${metrics}"
    printf '%s\n' "${stats}"
    exit 0
  fi
  sleep 0.5
done

printf '%s\n' "analytics did not process both events" >&2
exit 1
