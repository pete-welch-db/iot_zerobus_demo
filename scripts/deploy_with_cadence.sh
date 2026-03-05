#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-dev}"
MODE="${MODE:-demo}" # demo|steady
LAKEBASE_ENV_FILE="${LAKEBASE_ENV_FILE:-lakebase.env}"
GENIE_SPACE_NAME="${GENIE_SPACE_NAME:-Manufacturing Command Center}"
APP_GENIE_SPACE_ID="${APP_GENIE_SPACE_ID:-${GENIE_SPACE_ID:-}}"
DEFAULT_GENIE_SPACE_ID="01f117215c6112179fbec6269981f89b"

if [[ "$MODE" == "demo" ]]; then
  TELEMETRY_LIVE_WINDOW_HOURS="24"
else
  TELEMETRY_LIVE_WINDOW_HOURS="24"
fi

if [[ -f "$LAKEBASE_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$LAKEBASE_ENV_FILE"
fi

required_lakebase_keys=(
  "LAKEBASE_INSTANCE_ID"
  "LAKEBASE_RESOURCE_NAME"
  "LAKEBASE_DB_HOST"
  "LAKEBASE_DB_PORT"
  "LAKEBASE_DB_NAME"
  "LAKEBASE_SECRET_SCOPE"
  "LAKEBASE_USER_SECRET_KEY"
  "LAKEBASE_PASSWORD_SECRET_KEY"
  "LAKEBASE_JDBC_URL"
)
for key in "${required_lakebase_keys[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    echo "Missing required Lakebase contract key: $key" >&2
    echo "Run scripts/provision_lakebase_autoscaling.sh first (or export all keys)." >&2
    exit 1
  fi
done

# Slack feature disabled and archived under z_archive/slack/.

if [[ -z "${APP_GENIE_SPACE_ID:-}" || "${APP_GENIE_SPACE_ID:-}" == "__AUTO__" ]]; then
  echo "==> Resolving Genie space ID for '${GENIE_SPACE_NAME}' via Databricks API"
  RESOLVED_GENIE_SPACE_ID="$(GENIE_SPACE_NAME="$GENIE_SPACE_NAME" python3 - <<'PY'
import json
import os
import re
import subprocess
import sys

target = re.sub(r"[^a-z0-9]+", "", os.environ.get("GENIE_SPACE_NAME", "").lower())
token = None
seen = set()
while True:
    path = "/api/2.0/data-rooms?page_size=100"
    if token:
        path = f"/api/2.0/data-rooms?page_size=100&page_token={token}"
    cmd = ["databricks", "api", "get", path]
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        print("", end="")
        sys.exit(0)
    payload = json.loads(proc.stdout or "{}")
    spaces = payload.get("spaces") or payload.get("data_rooms") or payload.get("dataRooms") or []
    for space in spaces:
        title = space.get("display_name") or space.get("title") or space.get("name") or ""
        normalized = re.sub(r"[^a-z0-9]+", "", title.lower())
        if normalized == target:
            print(space.get("space_id") or space.get("id") or "", end="")
            sys.exit(0)
    token = payload.get("next_page_token") or payload.get("nextPageToken")
    if not token or token in seen:
        print("", end="")
        sys.exit(0)
    seen.add(token)
PY
)"
  if [[ -n "$RESOLVED_GENIE_SPACE_ID" ]]; then
    APP_GENIE_SPACE_ID="$RESOLVED_GENIE_SPACE_ID"
  fi
fi

if [[ -z "${APP_GENIE_SPACE_ID:-}" || "${APP_GENIE_SPACE_ID:-}" == "__AUTO__" ]]; then
  APP_GENIE_SPACE_ID="$DEFAULT_GENIE_SPACE_ID"
fi
echo "==> Using APP_GENIE_SPACE_ID=${APP_GENIE_SPACE_ID}"

echo "==> Deploying bundle with cadence mode: $MODE"
databricks bundle deploy -t "$TARGET" \
  --var "dashboard_mode=$MODE" \
  --var "telemetry_live_window_hours=$TELEMETRY_LIVE_WINDOW_HOURS" \
  --var "lakebase_instance_id=${LAKEBASE_INSTANCE_ID:-}" \
  --var "lakebase_resource_name=${LAKEBASE_RESOURCE_NAME:-}" \
  --var "lakebase_db_host=${LAKEBASE_DB_HOST:-}" \
  --var "lakebase_db_port=${LAKEBASE_DB_PORT:-5432}" \
  --var "lakebase_db_name=${LAKEBASE_DB_NAME:-iot_demo}" \
  --var "lakebase_secret_scope=${LAKEBASE_SECRET_SCOPE:-iot_zerobus_demo}" \
  --var "lakebase_user_secret_key=${LAKEBASE_USER_SECRET_KEY:-lakebase_db_user}" \
  --var "lakebase_password_secret_key=${LAKEBASE_PASSWORD_SECRET_KEY:-lakebase_db_password}" \
  --var "lakebase_jdbc_url=${LAKEBASE_JDBC_URL:-}" \
  --var "genie_space_id=${APP_GENIE_SPACE_ID}" \
  --var "app_genie_space_id=${APP_GENIE_SPACE_ID}"

echo "deploy with cadence phase complete."
