#!/usr/bin/env bash

set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
RUNS="${RUNS:-5}"
WARMUP="${WARMUP:-1}"
DELAY_MS="${DELAY_MS:-150}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

usage() {
  cat <<'EOF'
Uso:
  BASE_URL=http://127.0.0.1:8080 RUNS=7 ./scripts/profile-api.sh

Variabili supportate:
  BASE_URL     Base URL del backend (default: http://127.0.0.1:8080)
  RUNS         Numero di run misurate per endpoint (default: 5)
  WARMUP       Numero di warmup per endpoint (default: 1)
  DELAY_MS     Pausa tra le run in millisecondi (default: 150)

Endpoint opzionali:
  STOP_ID      Se valorizzato, include /api/v1/stops/{STOP_ID}/arrivals
  TRIP_ID      Se valorizzato, include /api/v1/trips/{TRIP_ID}/shape e /stops
  TRAIN_NUMBER Se valorizzato, include /api/v1/rail/train-info

Esempio:
  BASE_URL=http://127.0.0.1:8080 STOP_ID=70059 TRAIN_NUMBER=20833 ./scripts/profile-api.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || (( RUNS < 1 )); then
  echo "RUNS deve essere un intero > 0" >&2
  exit 1
fi

if ! [[ "$WARMUP" =~ ^[0-9]+$ ]] || (( WARMUP < 0 )); then
  echo "WARMUP deve essere un intero >= 0" >&2
  exit 1
fi

if ! [[ "$DELAY_MS" =~ ^[0-9]+$ ]] || (( DELAY_MS < 0 )); then
  echo "DELAY_MS deve essere un intero >= 0" >&2
  exit 1
fi

sleep_ms() {
  local ms="$1"
  if (( ms <= 0 )); then
    return
  fi
  python3 - <<PY
import time
time.sleep(${ms} / 1000)
PY
}

declare -a CASES=()

add_case() {
  local name="$1"
  local path="$2"
  CASES+=("${name}|${path}")
}

add_case "dashboard" "/api/v1/dashboard/summary"
add_case "vehicles" "/api/v1/vehicles"
add_case "geocode_termini" "/api/v1/geocode/search?q=termini&limit=5"
add_case "nearby_centro" "/api/v1/nearby?lat=41.9028&lon=12.4964&radiusMeters=500&limitStops=8&limitArrivalsPerStop=6"
add_case "journey_bus_metro" "/api/v1/journey/plan?fromLat=41.9028&fromLon=12.4964&toLat=41.9142&toLon=12.4596&modes=BUS,SUBWAY,TRAM,RAIL"

if [[ -n "${STOP_ID:-}" ]]; then
  add_case "stop_arrivals_${STOP_ID}" "/api/v1/stops/${STOP_ID}/arrivals?limitArrivals=8"
fi

if [[ -n "${TRIP_ID:-}" ]]; then
  add_case "trip_shape" "/api/v1/trips/${TRIP_ID}/shape"
  add_case "trip_stops" "/api/v1/trips/${TRIP_ID}/stops"
fi

if [[ -n "${TRAIN_NUMBER:-}" ]]; then
  add_case "rail_train_info_${TRAIN_NUMBER}" "/api/v1/rail/train-info?trainNumber=${TRAIN_NUMBER}"
fi

run_probe() {
  local url="$1"
  local run_id="$2"
  local out_file="${TMP_DIR}/body-${run_id}.tmp"

  curl --silent --show-error --location \
    --output "$out_file" \
    --write-out '%{http_code};%{time_namelookup};%{time_connect};%{time_starttransfer};%{time_total};%{size_download}\n' \
    "$url"
}

print_header() {
  printf '\n%-24s %6s %10s %10s %10s %10s %10s %12s\n' \
    "endpoint" "http" "avg_ms" "p95_ms" "min_ms" "max_ms" "ttfb_ms" "bytes_avg"
  printf '%s\n' "--------------------------------------------------------------------------------------------------------"
}

profile_case() {
  local name="$1"
  local path="$2"
  local url="${BASE_URL}${path}"
  local metrics_file="${TMP_DIR}/${name}.csv"
  : > "$metrics_file"
  local failed=0

  local i
  for (( i=1; i<=WARMUP; i++ )); do
    if ! run_probe "$url" "${name}-warmup-${i}" >/dev/null 2>&1; then
      failed=1
      break
    fi
    sleep_ms "$DELAY_MS"
  done

  if (( failed == 1 )); then
    printf '%-24s %6s %10s %10s %10s %10s %10s %12s\n' "$name" "ERR" "-" "-" "-" "-" "-" "-"
    return
  fi

  for (( i=1; i<=RUNS; i++ )); do
    local raw
    if ! raw="$(run_probe "$url" "${name}-${i}" 2>/dev/null)"; then
      failed=1
      break
    fi
    printf '%s\n' "$raw" >> "$metrics_file"
    sleep_ms "$DELAY_MS"
  done

  if (( failed == 1 )); then
    printf '%-24s %6s %10s %10s %10s %10s %10s %12s\n' "$name" "ERR" "-" "-" "-" "-" "-" "-"
    return
  fi

  python3 - "$name" "$metrics_file" <<'PY'
import csv
import math
import sys

endpoint = sys.argv[1]
metrics_file = sys.argv[2]

rows = []
with open(metrics_file, newline="") as handle:
    reader = csv.reader(handle, delimiter=";")
    for row in reader:
        if row:
            rows.append(row)

if not rows:
    print(f"{endpoint:<24} {'ERR':>6} {'-':>10} {'-':>10} {'-':>10} {'-':>10} {'-':>10} {'-':>12}")
    sys.exit(0)

http = rows[-1][0]
totals = sorted(float(row[4]) for row in rows)
ttfbs = [float(row[3]) for row in rows]
sizes = [float(row[5]) for row in rows]

count = len(rows)
p95_index = max(0, min(count - 1, math.ceil(count * 0.95) - 1))

def round_ms(value: float) -> int:
    return int((value * 1000) + 0.5)

avg_total = sum(totals) / count
avg_ttfb = sum(ttfbs) / count
avg_bytes = int((sum(sizes) / count) + 0.5)

print(
    f"{endpoint:<24} {http:>6} "
    f"{round_ms(avg_total):>10} {round_ms(totals[p95_index]):>10} "
    f"{round_ms(totals[0]):>10} {round_ms(totals[-1]):>10} "
    f"{round_ms(avg_ttfb):>10} {avg_bytes:>12}"
)
PY
}

print_header

for entry in "${CASES[@]}"; do
  name="${entry%%|*}"
  path="${entry#*|}"
  profile_case "$name" "$path"
done

cat <<EOF

Base URL: ${BASE_URL}
Run per endpoint: ${RUNS} (+ ${WARMUP} warmup)
Pausa tra run: ${DELAY_MS} ms

Lettura rapida:
- Se cresce molto il TTFB, il collo di bottiglia e' quasi sempre backend/index/query upstream.
- Se TTFB e total sono simili, il tempo e' quasi tutto server-side.
- Se total >> TTFB, pesa il payload o il trasferimento.
EOF
