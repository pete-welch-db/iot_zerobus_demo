"""Set up Lakebase synced tables for the IoT demo.

Supports two modes:
  - CONTINUOUS: Syncs gold_machine_current_status (CDF-enabled streaming table)
    for near-real-time telemetry (~15s latency). ML enrichment is handled app-side.
  - SNAPSHOT:   Syncs vw_machine_current_status (view with ML + dim joins).
    Requires scheduled refresh since views don't support CDF.

Prerequisites:
  pip install databricks-sdk

Usage:
  # Create CONTINUOUS synced table (recommended):
  python lakebase_synced_tables_setup.py --mode continuous

  # Create SNAPSHOT synced table (fallback):
  python lakebase_synced_tables_setup.py --mode snapshot

  # Delete existing synced table (before recreating with different mode):
  python lakebase_synced_tables_setup.py --delete

  # Check sync status:
  python lakebase_synced_tables_setup.py --status

  # Dry run:
  python lakebase_synced_tables_setup.py --mode continuous --dry-run

  # Clean up old mirror tables from public schema:
  python lakebase_synced_tables_setup.py --cleanup-old-tables
"""

import argparse
import logging
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    NewPipelineSpec,
    SyncedDatabaseTable,
    SyncedTableSchedulingPolicy,
    SyncedTableSpec,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CATALOG = "welch"
SCHEMA = "iot_demo_dev"
LAKEBASE_INSTANCE = "iot-demo-lakebase"
LOGICAL_DATABASE = "iot_demo"

# Source table depends on mode
SOURCE_TABLES = {
    "continuous": "gold_machine_latest_status",   # Deduplicated, CDF-enabled, near-real-time
    "snapshot": "vw_machine_current_status",       # View with ML enrichment
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set up Lakebase synced tables for IoT demo."
    )
    parser.add_argument("--catalog", default=CATALOG)
    parser.add_argument("--schema", default=SCHEMA)
    parser.add_argument("--lakebase-instance", default=LAKEBASE_INSTANCE)
    parser.add_argument("--logical-database", default=LOGICAL_DATABASE)
    parser.add_argument(
        "--mode",
        choices=["continuous", "snapshot"],
        default="continuous",
        help="Sync mode: 'continuous' for real-time from gold table, "
             "'snapshot' for periodic from enriched view (default: continuous).",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete existing synced table and drop Lakebase table.",
    )
    parser.add_argument(
        "--cleanup-old-tables",
        action="store_true",
        help="Drop old mirror-created tables from the public schema.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check sync status of existing synced table and exit.",
    )
    return parser.parse_args()


def create_synced_table(w: WorkspaceClient, args: argparse.Namespace) -> None:
    """Create the machine_current_status synced table."""
    catalog = args.catalog
    schema = args.schema
    dest_name = f"{catalog}.{schema}.machine_current_status"
    source_table = SOURCE_TABLES[args.mode]
    source_name = f"{catalog}.{schema}.{source_table}"
    policy = (
        SyncedTableSchedulingPolicy.CONTINUOUS
        if args.mode == "continuous"
        else SyncedTableSchedulingPolicy.SNAPSHOT
    )

    logger.info(
        f"{'[DRY RUN] Would create' if args.dry_run else 'Creating'} synced table:"
    )
    logger.info(f"  Source:   {source_name}")
    logger.info(f"  Dest UC:  {dest_name}")
    logger.info(f"  Instance: {args.lakebase_instance}")
    logger.info(f"  Database: {args.logical_database}")
    logger.info(f"  Mode:     {args.mode.upper()}")

    if args.dry_run:
        return

    try:
        existing = w.database.get_synced_database_table(name=dest_name)
        state = existing.data_synchronization_status.detailed_state
        logger.info(f"Synced table already exists (state: {state}). Skipping create.")
        logger.info("Use --delete first to recreate with a different mode.")
        return
    except Exception:
        pass

    result = w.database.create_synced_database_table(
        SyncedDatabaseTable(
            name=dest_name,
            database_instance_name=args.lakebase_instance,
            logical_database_name=args.logical_database,
            spec=SyncedTableSpec(
                source_table_full_name=source_name,
                primary_key_columns=["machine_id"],
                scheduling_policy=policy,
                create_database_objects_if_missing=True,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=catalog,
                    storage_schema=schema,
                ),
            ),
        )
    )
    logger.info(f"Created synced table: {result.name}")


def delete_synced_table(w: WorkspaceClient, args: argparse.Namespace) -> None:
    """Delete the synced table and drop the Lakebase Postgres table."""
    import uuid

    catalog = args.catalog
    schema = args.schema
    dest_name = f"{catalog}.{schema}.machine_current_status"

    # Delete the UC synced table definition
    logger.info(f"Deleting synced table: {dest_name}")
    try:
        w.database.delete_synced_database_table(name=dest_name)
        logger.info("  Synced table deleted from Unity Catalog.")
    except Exception as e:
        logger.warning(f"  Could not delete synced table: {e}")

    # Drop the Postgres table in Lakebase
    logger.info("Dropping Lakebase Postgres table...")
    try:
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[args.lakebase_instance],
        )
        instance = w.database.get_database_instance(name=args.lakebase_instance)

        import psycopg
        conninfo = (
            f"host={instance.read_write_dns} port=5432 "
            f"dbname={args.logical_database} "
            f"user={w.current_user.me().user_name} "
            f"password={cred.token} sslmode=require"
        )
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                # Try both possible schemas
                for pg_schema in [args.schema, "public"]:
                    try:
                        cur.execute(
                            f"DROP TABLE IF EXISTS {pg_schema}.machine_current_status CASCADE"
                        )
                        logger.info(f"  Dropped {pg_schema}.machine_current_status")
                    except Exception as e:
                        logger.warning(f"  Could not drop {pg_schema}.machine_current_status: {e}")
            conn.commit()
    except Exception as e:
        logger.warning(f"  Could not connect to Lakebase to drop table: {e}")

    logger.info("Delete complete. You can now recreate with --mode continuous or --mode snapshot.")


def check_status(w: WorkspaceClient, args: argparse.Namespace) -> None:
    """Poll the synced table status."""
    dest_name = f"{args.catalog}.{args.schema}.machine_current_status"
    try:
        status = w.database.get_synced_database_table(name=dest_name)
        sync = status.data_synchronization_status
        logger.info(f"Synced table: {dest_name}")
        logger.info(f"  State:   {sync.detailed_state}")
        logger.info(f"  Message: {sync.message or 'n/a'}")
        if hasattr(status, 'spec') and status.spec:
            logger.info(f"  Source:  {status.spec.source_table_full_name}")
            logger.info(f"  Policy:  {status.spec.scheduling_policy}")
    except Exception as e:
        logger.warning(f"Could not get status for {dest_name}: {e}")


def cleanup_old_tables(w: WorkspaceClient, args: argparse.Namespace) -> None:
    """Drop old mirror-created tables from the public schema in Lakebase."""
    import uuid

    logger.info("Cleaning up old mirror tables from public schema...")

    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[args.lakebase_instance],
    )
    instance = w.database.get_database_instance(name=args.lakebase_instance)

    import psycopg
    conninfo = (
        f"host={instance.read_write_dns} port=5432 "
        f"dbname={args.logical_database} "
        f"user={w.current_user.me().user_name} "
        f"password={cred.token} sslmode=require"
    )

    old_tables = [
        "public.machine_current_status",
        "public.dim_machine",
        "public.mirror_metadata",
    ]

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            for table in old_tables:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    logger.info(f"  Dropped {table}")
                except Exception as e:
                    logger.warning(f"  Could not drop {table}: {e}")
        conn.commit()

    logger.info("Cleanup complete.")


def main() -> None:
    args = parse_args()
    w = WorkspaceClient()

    logger.info("=" * 60)
    logger.info("LAKEBASE SYNCED TABLE SETUP")
    logger.info("=" * 60)

    if args.status:
        check_status(w, args)
        return

    if args.delete:
        delete_synced_table(w, args)
        return

    if args.cleanup_old_tables:
        cleanup_old_tables(w, args)
        return

    create_synced_table(w, args)

    if not args.dry_run:
        logger.info("Waiting 10s for initial sync status...")
        time.sleep(10)
        check_status(w, args)

    logger.info("=" * 60)
    logger.info("SETUP COMPLETE")
    logger.info("=" * 60)
    if not args.dry_run:
        if args.mode == "continuous":
            logger.info(
                "CONTINUOUS mode: Lakebase will receive near-real-time updates (~15s)."
            )
            logger.info(
                "ML enrichment (OEE, anomaly, fault risk) is handled app-side via cached SQL Warehouse query."
            )
        else:
            logger.info(
                "SNAPSHOT mode: Run --status to verify ONLINE, then schedule periodic refreshes."
            )
        logger.info(
            "Once status is ONLINE, run with --cleanup-old-tables to drop "
            "the old mirror tables from the public schema."
        )


if __name__ == "__main__":
    main()
