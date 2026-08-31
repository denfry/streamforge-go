#!/usr/bin/env bash
set -euo pipefail

base_url="${STREAMFORGE_BASE_URL:-http://localhost:8080}"
campaign_json="$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d '{"name":"Smoke campaign","daily_budget":100,"currency":"USD"}' \
  "${base_url}/v1/campaigns")"
campaign_id="$(jq -er '.id' <<<"${campaign_json}")"

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
  if jq -e '.impressions == 1 and .clicks == 1' <<<"${stats}" >/dev/null; then
    jq -e '.status == "ok"' <<<"$(curl --fail --silent --show-error "${base_url}/health")" >/dev/null
    jq -e 'length > 0' <<<"$(curl --fail --silent --show-error "${base_url}/metrics")" >/dev/null
    printf '%s\n' "${stats}"
    exit 0
  fi
  sleep 0.5
done

printf '%s\n' "analytics did not process both events" >&2
exit 1
