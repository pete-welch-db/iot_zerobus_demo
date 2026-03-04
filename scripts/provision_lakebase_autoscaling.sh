#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-dev}"
PROFILE="${PROFILE:-}"
PROJECT_ID="${LAKEBASE_PROJECT_ID:-iot-demo-lakebase}"
RESOURCE_NAME="${LAKEBASE_RESOURCE_NAME:-$PROJECT_ID}"
DB_NAME="${LAKEBASE_DB_NAME:-iot_demo}"
DB_PORT="${LAKEBASE_DB_PORT:-5432}"
SECRET_SCOPE="${LAKEBASE_SECRET_SCOPE:-iot_zerobus_demo}"
USER_SECRET_KEY="${LAKEBASE_USER_SECRET_KEY:-lakebase_db_user}"
PASSWORD_SECRET_KEY="${LAKEBASE_PASSWORD_SECRET_KEY:-lakebase_db_password}"
OUTPUT_FILE="${OUTPUT_FILE:-lakebase.env}"
MIN_CU="${LAKEBASE_MIN_CU:-0.5}"
MAX_CU="${LAKEBASE_MAX_CU:-2.0}"

PROFILE_ARGS=()
if [[ -n "$PROFILE" ]]; then
  PROFILE_ARGS=(-p "$PROFILE")
fi

dbx() {
  if [[ -n "$PROFILE" ]]; then
    databricks "$@" -p "$PROFILE"
  else
    databricks "$@"
  fi
}

echo "==> Ensuring Lakebase autoscaling project exists: $PROJECT_ID"
if databricks postgres -h >/dev/null 2>&1; then
  if ! dbx postgres get-project "projects/$PROJECT_ID" >/dev/null 2>&1; then
    dbx postgres create-project "$PROJECT_ID" \
      --json "{\"spec\": {\"display_name\": \"$RESOURCE_NAME\"}}" \
      >/dev/null
  fi

  echo "==> Ensuring autoscaling limits on primary endpoint"
  dbx postgres update-endpoint \
    "projects/$PROJECT_ID/branches/production/endpoints/primary" \
    "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu" \
    --json "{\"spec\": {\"autoscaling_limit_min_cu\": $MIN_CU, \"autoscaling_limit_max_cu\": $MAX_CU}}" >/dev/null || true

  echo "==> Resolving Lakebase endpoint host"
  HOST="$(dbx postgres list-endpoints "projects/$PROJECT_ID/branches/production" -o json | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d[0].get("status",{}).get("hosts",{}) or {}).get("host",""))')"
  if [[ -z "$HOST" ]]; then
    echo "Failed to resolve Lakebase endpoint host." >&2
    exit 1
  fi
  INSTANCE_ID="projects/$PROJECT_ID"
else
  # Legacy Lakebase CLI fallback when autoscaling postgres commands are unavailable.
  if ! dbx database get-database-instance "$RESOURCE_NAME" >/dev/null 2>&1; then
    dbx database create-database-instance "$RESOURCE_NAME" --capacity "CU_2" >/dev/null
  fi
  INSTANCE_JSON="$(dbx database get-database-instance "$RESOURCE_NAME" -o json)"
  HOST="$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(d.get("read_write_dns",""))' "$INSTANCE_JSON")"
  INSTANCE_ID="$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(d.get("uid",""))' "$INSTANCE_JSON")"
  if [[ -z "$HOST" || -z "$INSTANCE_ID" ]]; then
    echo "Failed to resolve legacy Lakebase instance metadata." >&2
    exit 1
  fi
fi

JDBC_URL="jdbc:postgresql://$HOST:$DB_PORT/$DB_NAME"

cat > "$OUTPUT_FILE" <<EOF
LAKEBASE_INSTANCE_ID=$INSTANCE_ID
LAKEBASE_RESOURCE_NAME=$RESOURCE_NAME
LAKEBASE_DB_HOST=$HOST
LAKEBASE_DB_PORT=$DB_PORT
LAKEBASE_DB_NAME=$DB_NAME
LAKEBASE_SECRET_SCOPE=$SECRET_SCOPE
LAKEBASE_USER_SECRET_KEY=$USER_SECRET_KEY
LAKEBASE_PASSWORD_SECRET_KEY=$PASSWORD_SECRET_KEY
LAKEBASE_JDBC_URL=$JDBC_URL
EOF

required=(
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
for key in "${required[@]}"; do
  if ! grep -q "^${key}=." "$OUTPUT_FILE"; then
    echo "Missing required Lakebase output key: $key" >&2
    exit 1
  fi
done

echo "==> Lakebase contract written to $OUTPUT_FILE"
echo "provision lakebase phase complete."
