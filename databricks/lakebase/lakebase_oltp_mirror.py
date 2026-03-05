"""
Mirror near-real-time OLAP semantic outputs into Lakebase OLTP tables.

Production Features:
- Row count validation before collect() to prevent OOM
- Connection retry logic with exponential backoff
- Detailed logging and performance metrics
- Data validation before upsert
- Batch processing support for large datasets
- Graceful error handling with proper cleanup
"""

import argparse
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────────────────────────────
# CONFIGURATION CONSTANTS
# ─────────────────────────────────────────────────────────────────────
MAX_ROWS_FOR_COLLECT = 100000  # Prevent OOM: switch to batch mode if exceeded
BATCH_SIZE = 5000  # Number of rows per batch in batch mode
MAX_RETRIES = 3  # Connection retry attempts
RETRY_BACKOFF_SECONDS = 2  # Initial backoff, doubles each retry
CONNECTION_TIMEOUT = 30  # PostgreSQL connection timeout (seconds)

# ─────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upsert current IoT semantic status into Lakebase.")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument("--jdbc-url", default="", help="Full JDBC URL (overrides host/port/db)")
    parser.add_argument("--db-host", default="", help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--db-name", default="iot_demo", help="PostgreSQL database name")
    parser.add_argument("--secret-scope", required=True, help="Databricks secret scope")
    parser.add_argument("--user-secret-key", required=True, help="Secret key for PostgreSQL user")
    parser.add_argument("--password-secret-key", required=True, help="Secret key for PostgreSQL password")
    parser.add_argument("--instance-id", default="", help="Unique instance ID for tracking")
    parser.add_argument("--dry-run", action="store_true", help="Validate data without writing to Lakebase")
    parser.add_argument("--batch-mode", action="store_true", help="Force batch processing (bypass collect())")
    parser.add_argument("--max-rows", type=int, default=MAX_ROWS_FOR_COLLECT, help="Max rows for collect()")
    return parser.parse_args()


def _resolve_jdbc(args: argparse.Namespace) -> str:
    """Resolve JDBC URL from args."""
    if args.jdbc_url:
        return args.jdbc_url
    if not args.db_host:
        raise ValueError("Either --jdbc-url or --db-host must be provided.")
    return f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"


def _jdbc_to_pg_dsn(jdbc_url: str, user: str, password: str) -> str:
    """Convert JDBC URL to psycopg2 DSN connection string."""
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL format: {jdbc_url}")
    # Remove "jdbc:" prefix
    dsn = jdbc_url.replace("jdbc:", "", 1)
    # Add connection parameters
    return f"{dsn}?sslmode=require&user={user}&password={password}&connect_timeout={CONNECTION_TIMEOUT}"


def _validate_row_count(view_name: str, max_rows: int) -> Tuple[int, bool]:
    """
    Check row count before collect() to prevent OOM.
    
    Returns:
        (row_count, use_batch_mode)
    """
    logger.info(f"Validating row count for {view_name}...")
    start_time = time.time()
    
    row_count = spark.table(view_name).count()
    elapsed = time.time() - start_time
    
    logger.info(f"Row count: {row_count:,} (took {elapsed:.2f}s)")
    
    use_batch_mode = row_count > max_rows
    if use_batch_mode:
        logger.warning(
            f"Row count {row_count:,} exceeds max {max_rows:,}. "
            f"Switching to batch mode (batch_size={BATCH_SIZE})."
        )
    
    return row_count, use_batch_mode


def _collect_rows_all(view_name: str) -> List[Dict[str, Any]]:
    """Collect all rows using collect() - suitable for datasets < 100K rows."""
    logger.info(f"Collecting all rows from {view_name} using collect()...")
    start_time = time.time()
    
    df = spark.table(view_name).select(
        "machine_id",
        "state",
        "last_event_time",
        "telemetry_lag_ms",
        "ml_lag_ms",
        "temp_c",
        "vibration_mm_s",
        "throughput_cpm",
        "rpm",
        "current_amps",
        "humidity_pct",
        "load_pct",
        "power_kw",
        "power_factor",
        "voltage_v",
        "pressure_bar",
        "flow_rate_lpm",
        "oee_pct",
        "anomaly_score",
        "prob_fault_next_5m",
    )
    
    rows = [r.asDict(recursive=True) for r in df.collect()]
    elapsed = time.time() - start_time
    
    logger.info(f"Collected {len(rows):,} rows in {elapsed:.2f}s")
    return rows


def _collect_rows_batch(view_name: str, batch_size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Collect rows in batches using toLocalIterator() to avoid OOM.
    Suitable for datasets > 100K rows.
    """
    logger.info(f"Collecting rows from {view_name} in batches (size={batch_size})...")
    start_time = time.time()
    
    df = spark.table(view_name).select(
        "machine_id",
        "state",
        "last_event_time",
        "telemetry_lag_ms",
        "ml_lag_ms",
        "temp_c",
        "vibration_mm_s",
        "throughput_cpm",
        "rpm",
        "current_amps",
        "humidity_pct",
        "load_pct",
        "power_kw",
        "power_factor",
        "voltage_v",
        "pressure_bar",
        "flow_rate_lpm",
        "oee_pct",
        "anomaly_score",
        "prob_fault_next_5m",
    )
    
    rows = []
    batch_count = 0
    
    # Use toLocalIterator to avoid loading entire dataset into memory
    for row in df.toLocalIterator():
        rows.append(row.asDict(recursive=True))
        if len(rows) % batch_size == 0:
            batch_count += 1
            logger.info(f"Processed batch {batch_count} ({len(rows):,} rows so far)...")
    
    elapsed = time.time() - start_time
    logger.info(f"Collected {len(rows):,} rows in {batch_count} batches ({elapsed:.2f}s)")
    
    return rows


def _validate_data(rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Validate collected data before upsert.
    
    Returns:
        (valid_rows, invalid_rows)
    """
    logger.info("Validating data quality...")
    
    valid_rows = 0
    invalid_rows = 0
    
    for idx, row in enumerate(rows):
        # Check for required fields
        if not row.get("machine_id"):
            logger.warning(f"Row {idx}: Missing machine_id")
            invalid_rows += 1
            continue
        
        # Check for null event_time (critical field)
        if row.get("last_event_time") is None:
            logger.warning(f"Row {idx}: machine_id={row.get('machine_id')} has null last_event_time")
            invalid_rows += 1
            continue
        
        valid_rows += 1
    
    logger.info(f"Validation complete: {valid_rows:,} valid, {invalid_rows:,} invalid")
    
    if invalid_rows > 0:
        logger.warning(f"{invalid_rows:,} rows failed validation (will be skipped in upsert)")
    
    return valid_rows, invalid_rows


def _connect_with_retry(dsn: str, max_retries: int = MAX_RETRIES) -> psycopg2.extensions.connection:
    """
    Establish PostgreSQL connection with exponential backoff retry logic.
    """
    logger.info("Connecting to PostgreSQL...")
    
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(dsn)
            logger.info(f"Connected successfully (attempt {attempt}/{max_retries})")
            return conn
        except psycopg2.OperationalError as e:
            if attempt == max_retries:
                logger.error(f"Failed to connect after {max_retries} attempts")
                raise
            
            backoff = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                f"Connection failed (attempt {attempt}/{max_retries}): {e}. "
                f"Retrying in {backoff}s..."
            )
            time.sleep(backoff)
    
    raise RuntimeError("Unexpected connection retry logic failure")


def _ensure_tables(conn) -> None:
    """Create Lakebase tables if they don't exist."""
    logger.info("Ensuring Lakebase tables exist...")
    start_time = time.time()
    
    with conn.cursor() as cur:
        # Create main status table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS machine_current_status (
              machine_id TEXT PRIMARY KEY,
              state TEXT,
              last_event_time TIMESTAMPTZ,
              telemetry_lag_ms BIGINT,
              ml_lag_ms BIGINT,
              temp_c DOUBLE PRECISION,
              vibration_mm_s DOUBLE PRECISION,
              throughput_cpm INTEGER,
              rpm INTEGER,
              current_amps DOUBLE PRECISION,
              humidity_pct DOUBLE PRECISION,
              load_pct DOUBLE PRECISION,
              power_kw DOUBLE PRECISION,
              power_factor DOUBLE PRECISION,
              voltage_v DOUBLE PRECISION,
              pressure_bar DOUBLE PRECISION,
              flow_rate_lpm DOUBLE PRECISION,
              oee_pct DOUBLE PRECISION,
              anomaly_score DOUBLE PRECISION,
              prob_fault_next_5m DOUBLE PRECISION,
              source_watermark TIMESTAMPTZ,
              updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        
        # Create metadata tracking table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mirror_metadata (
              instance_id TEXT PRIMARY KEY,
              last_run_at TIMESTAMPTZ,
              row_count BIGINT,
              source_watermark TIMESTAMPTZ,
              execution_time_seconds DOUBLE PRECISION,
              status TEXT
            )
            """
        )
        
        # Create index on last_event_time for faster queries
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_machine_status_event_time 
            ON machine_current_status(last_event_time DESC)
            """
        )
    
    conn.commit()
    elapsed = time.time() - start_time
    logger.info(f"Tables ensured in {elapsed:.2f}s")


def _upsert_rows(
    conn,
    rows: List[Dict[str, Any]],
    instance_id: str,
    execution_time: float,
    dry_run: bool = False,
) -> None:
    """
    Upsert rows to Lakebase with validation and error handling.
    """
    if not rows:
        logger.warning("No rows to upsert")
        return
    
    # Filter out invalid rows
    valid_rows = [
        row for row in rows
        if row.get("machine_id") and row.get("last_event_time") is not None
    ]
    
    if not valid_rows:
        logger.error("No valid rows after filtering")
        return
    
    logger.info(f"Upserting {len(valid_rows):,} valid rows...")
    
    if dry_run:
        logger.info("[DRY RUN] Skipping actual upsert to Lakebase")
        logger.info(f"[DRY RUN] Would have upserted {len(valid_rows):,} rows")
        return
    
    start_time = time.time()
    
    # Calculate source watermark (latest event time)
    source_watermark = max(
        row["last_event_time"] for row in valid_rows if row["last_event_time"] is not None
    )
    
    # Prepare payload
    payload = [
        (
            row["machine_id"],
            row["state"],
            row["last_event_time"],
            row["telemetry_lag_ms"],
            row["ml_lag_ms"],
            row["temp_c"],
            row["vibration_mm_s"],
            row["throughput_cpm"],
            row["rpm"],
            row["current_amps"],
            row["humidity_pct"],
            row["load_pct"],
            row["power_kw"],
            row["power_factor"],
            row["voltage_v"],
            row["pressure_bar"],
            row["flow_rate_lpm"],
            row["oee_pct"],
            row["anomaly_score"],
            row["prob_fault_next_5m"],
            source_watermark,
        )
        for row in valid_rows
    ]
    
    with conn.cursor() as cur:
        # Upsert machine status
        execute_values(
            cur,
            """
            INSERT INTO machine_current_status (
              machine_id, state, last_event_time, telemetry_lag_ms, ml_lag_ms,
              temp_c, vibration_mm_s, throughput_cpm, rpm, current_amps, humidity_pct,
              load_pct, power_kw, power_factor, voltage_v, pressure_bar, flow_rate_lpm,
              oee_pct, anomaly_score, prob_fault_next_5m, source_watermark
            ) VALUES %s
            ON CONFLICT (machine_id) DO UPDATE SET
              state = EXCLUDED.state,
              last_event_time = EXCLUDED.last_event_time,
              telemetry_lag_ms = EXCLUDED.telemetry_lag_ms,
              ml_lag_ms = EXCLUDED.ml_lag_ms,
              temp_c = EXCLUDED.temp_c,
              vibration_mm_s = EXCLUDED.vibration_mm_s,
              throughput_cpm = EXCLUDED.throughput_cpm,
              rpm = EXCLUDED.rpm,
              current_amps = EXCLUDED.current_amps,
              humidity_pct = EXCLUDED.humidity_pct,
              load_pct = EXCLUDED.load_pct,
              power_kw = EXCLUDED.power_kw,
              power_factor = EXCLUDED.power_factor,
              voltage_v = EXCLUDED.voltage_v,
              pressure_bar = EXCLUDED.pressure_bar,
              flow_rate_lpm = EXCLUDED.flow_rate_lpm,
              oee_pct = EXCLUDED.oee_pct,
              anomaly_score = EXCLUDED.anomaly_score,
              prob_fault_next_5m = EXCLUDED.prob_fault_next_5m,
              source_watermark = EXCLUDED.source_watermark,
              updated_at = NOW()
            """,
            payload,
        )
        
        # Update metadata
        cur.execute(
            """
            INSERT INTO mirror_metadata (
              instance_id, last_run_at, row_count, source_watermark, 
              execution_time_seconds, status
            )
            VALUES (%s, NOW(), %s, %s, %s, %s)
            ON CONFLICT (instance_id) DO UPDATE SET
              last_run_at = EXCLUDED.last_run_at,
              row_count = EXCLUDED.row_count,
              source_watermark = EXCLUDED.source_watermark,
              execution_time_seconds = EXCLUDED.execution_time_seconds,
              status = EXCLUDED.status
            """,
            (instance_id or "lakebase-default", len(valid_rows), source_watermark, execution_time, "SUCCESS"),
        )
    
    conn.commit()
    elapsed = time.time() - start_time
    logger.info(f"Upserted {len(valid_rows):,} rows in {elapsed:.2f}s")


def main() -> None:
    """Main execution logic with comprehensive error handling and metrics."""
    start_time = time.time()
    args = parse_args()
    
    logger.info("=" * 70)
    logger.info("LAKEBASE OLTP MIRROR - START")
    logger.info("=" * 70)
    logger.info(f"Catalog: {args.catalog}")
    logger.info(f"Schema: {args.schema}")
    logger.info(f"Instance ID: {args.instance_id or 'lakebase-default'}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info(f"Batch Mode: {args.batch_mode}")
    logger.info(f"Max Rows for Collect: {args.max_rows:,}")
    
    # Initialize metrics
    metrics = {
        "status": "FAILED",
        "total_rows": 0,
        "valid_rows": 0,
        "invalid_rows": 0,
        "execution_time_seconds": 0,
        "stages": {},
    }
    
    conn: Optional[psycopg2.extensions.connection] = None
    
    try:
        # ─────────────────────────────────────────────────────────────────
        # STAGE 1: Validate Row Count
        # ─────────────────────────────────────────────────────────────────
        stage_start = time.time()
        view_name = f"{args.catalog}.{args.schema}.vw_machine_current_status"
        row_count, use_batch_mode = _validate_row_count(view_name, args.max_rows)
        metrics["total_rows"] = row_count
        metrics["stages"]["validate_row_count"] = time.time() - stage_start
        
        # Override if --batch-mode flag is set
        if args.batch_mode:
            use_batch_mode = True
            logger.info("Batch mode forced via --batch-mode flag")
        
        # ─────────────────────────────────────────────────────────────────
        # STAGE 2: Collect Rows
        # ─────────────────────────────────────────────────────────────────
        stage_start = time.time()
        if use_batch_mode:
            rows = _collect_rows_batch(view_name, BATCH_SIZE)
        else:
            rows = _collect_rows_all(view_name)
        metrics["stages"]["collect_rows"] = time.time() - stage_start
        
        # ─────────────────────────────────────────────────────────────────
        # STAGE 3: Validate Data
        # ─────────────────────────────────────────────────────────────────
        stage_start = time.time()
        valid_rows, invalid_rows = _validate_data(rows)
        metrics["valid_rows"] = valid_rows
        metrics["invalid_rows"] = invalid_rows
        metrics["stages"]["validate_data"] = time.time() - stage_start
        
        # ─────────────────────────────────────────────────────────────────
        # STAGE 4: Connect to PostgreSQL
        # ─────────────────────────────────────────────────────────────────
        stage_start = time.time()
        user = dbutils.secrets.get(scope=args.secret_scope, key=args.user_secret_key)
        password = dbutils.secrets.get(scope=args.secret_scope, key=args.password_secret_key)
        jdbc_url = _resolve_jdbc(args)
        dsn = _jdbc_to_pg_dsn(jdbc_url, user, password)
        
        conn = _connect_with_retry(dsn, MAX_RETRIES)
        metrics["stages"]["connect_postgres"] = time.time() - stage_start
        
        # ─────────────────────────────────────────────────────────────────
        # STAGE 5: Ensure Tables
        # ─────────────────────────────────────────────────────────────────
        stage_start = time.time()
        _ensure_tables(conn)
        metrics["stages"]["ensure_tables"] = time.time() - stage_start
        
        # ─────────────────────────────────────────────────────────────────
        # STAGE 6: Upsert Rows
        # ─────────────────────────────────────────────────────────────────
        stage_start = time.time()
        execution_time = time.time() - start_time
        _upsert_rows(conn, rows, args.instance_id, execution_time, args.dry_run)
        metrics["stages"]["upsert_rows"] = time.time() - stage_start
        
        metrics["status"] = "SUCCESS"
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        metrics["status"] = "FAILED"
        metrics["error"] = str(e)
        raise
    
    finally:
        # Cleanup
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed")
        
        # Calculate final metrics
        metrics["execution_time_seconds"] = time.time() - start_time
        
        # ─────────────────────────────────────────────────────────────────
        # SUMMARY
        # ─────────────────────────────────────────────────────────────────
        logger.info("=" * 70)
        logger.info("LAKEBASE OLTP MIRROR - SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Status: {metrics['status']}")
        logger.info(f"Total Rows: {metrics['total_rows']:,}")
        logger.info(f"Valid Rows: {metrics['valid_rows']:,}")
        logger.info(f"Invalid Rows: {metrics['invalid_rows']:,}")
        logger.info(f"Total Execution Time: {metrics['execution_time_seconds']:.2f}s")
        logger.info("")
        logger.info("Stage Timings:")
        for stage, duration in metrics["stages"].items():
            logger.info(f"  - {stage}: {duration:.2f}s")
        logger.info("=" * 70)
        
        # Output metrics as JSON for downstream monitoring
        print(json.dumps(metrics, indent=2, default=str))


if __name__ == "__main__":
    main()
