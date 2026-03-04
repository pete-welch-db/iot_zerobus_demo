#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-dev}"
MODE="${MODE:-demo}" # demo|steady
LAKEBASE_ENV_FILE="${LAKEBASE_ENV_FILE:-lakebase.env}"

if [[ "$MODE" == "demo" ]]; then
  ML_CRON="0 0/1 * * * ?"
  MIRROR_CRON="0 0/1 * * * ?"
  DASHBOARD_REFRESH_SECONDS="60"
else
  ML_CRON="0 0/5 * * * ?"
  MIRROR_CRON="0 0/5 * * * ?"
  DASHBOARD_REFRESH_SECONDS="300"
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

echo "==> Deploying bundle with cadence mode: $MODE"
databricks bundle deploy -t "$TARGET" \
  --var "dashboard_mode=$MODE" \
  --var "ml_scoring_schedule_cron=$ML_CRON" \
  --var "oltp_mirror_schedule_cron=$MIRROR_CRON" \
  --var "dashboard_refresh_seconds_demo=$DASHBOARD_REFRESH_SECONDS" \
  --var "dashboard_refresh_seconds_steady=$DASHBOARD_REFRESH_SECONDS" \
  --var "lakebase_instance_id=${LAKEBASE_INSTANCE_ID:-}" \
  --var "lakebase_resource_name=${LAKEBASE_RESOURCE_NAME:-}" \
  --var "lakebase_db_host=${LAKEBASE_DB_HOST:-}" \
  --var "lakebase_db_port=${LAKEBASE_DB_PORT:-5432}" \
  --var "lakebase_db_name=${LAKEBASE_DB_NAME:-iot_demo}" \
  --var "lakebase_secret_scope=${LAKEBASE_SECRET_SCOPE:-iot_zerobus_demo}" \
  --var "lakebase_user_secret_key=${LAKEBASE_USER_SECRET_KEY:-lakebase_db_user}" \
  --var "lakebase_password_secret_key=${LAKEBASE_PASSWORD_SECRET_KEY:-lakebase_db_password}" \
  --var "lakebase_jdbc_url=${LAKEBASE_JDBC_URL:-}"

echo "==> Updating AI/BI dashboard schedule to ${DASHBOARD_REFRESH_SECONDS}s"
python3 scripts/configure_dashboard_schedule.py --interval-seconds "$DASHBOARD_REFRESH_SECONDS"

echo "deploy with cadence phase complete."
