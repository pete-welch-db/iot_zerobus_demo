"""Set up Lakebase synced tables for the IoT demo.

Replaces the custom lakebase_oltp_mirror.py with Databricks-managed
synced tables.  Snapshot mode syncs vw_machine_current_status (which
already includes line_name via JOIN with dim_machine) into Lakebase
automatically — no custom ETL code required.

Prerequisites:
  pip install databricks-sdk

Usage:
  # Dry run — show what would be created:
  python lakebase_synced_tables_setup.py --dry-run

  # Create the synced table:
  python lakebase_synced_tables_setup.py

  # After verifying synced table is ONLINE, clean up old mirror tables:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set up Lakebase synced tables for IoT demo."
    )
    parser.add_argument("--catalog", default=CATALOG)
    parser.add_argument("--schema", default=SCHEMA)
    parser.add_argument("--lakebase-instance", default=LAKEBASE_INSTANCE)
    parser.add_argument("--logical-database", default=LOGICAL_DATABASE)
    parser.add_argument("--dry-run", action="store_true")
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
    source_name = f"{catalog}.{schema}.vw_machine_current_status"

    logger.info(
        f"{'[DRY RUN] Would create' if args.dry_run else 'Creating'} synced table:"
    )
    logger.info(f"  Source:   {source_name}")
    logger.info(f"  Dest UC:  {dest_name}")
    logger.info(f"  Instance: {args.lakebase_instance}")
    logger.info(f"  Database: {args.logical_database}")
    logger.info(f"  Mode:     SNAPSHOT")

    if args.dry_run:
        return

    try:
        existing = w.database.get_synced_database_table(name=dest_name)
        state = existing.data_synchronization_status.detailed_state
        logger.info(f"Synced table already exists (state: {state}). Skipping create.")
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
                scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                create_database_objects_if_missing=True,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=catalog,
                    storage_schema=schema,
                ),
            ),
        )
    )
    logger.info(f"Created synced table: {result.name}")


def check_status(w: WorkspaceClient, args: argparse.Namespace) -> None:
    """Poll the synced table status."""
    dest_name = f"{args.catalog}.{args.schema}.machine_current_status"
    try:
        status = w.database.get_synced_database_table(name=dest_name)
        sync = status.data_synchronization_status
        logger.info(f"Synced table: {dest_name}")
        logger.info(f"  State:   {sync.detailed_state}")
        logger.info(f"  Message: {sync.message or 'n/a'}")
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
        logger.info(
            "Once status is ONLINE, run with --cleanup-old-tables to drop "
            "the old mirror tables from the public schema."
        )


if __name__ == "__main__":
    main()
