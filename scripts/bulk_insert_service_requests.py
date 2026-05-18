"""Bulk insert ~1M service request records into Lakebase using COPY protocol."""

import io
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from databricks.sdk import WorkspaceClient

INSTANCE_NAME = "iot-demo-lakebase"
DB_NAME = "iot_demo"

MACHINE_IDS = [f"MC-{i:04d}" for i in range(101)]
PRIORITIES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
PRIORITY_WEIGHTS = [5, 15, 50, 30]
REQUEST_TYPES = ["PREVENTIVE", "CORRECTIVE", "INSPECTION", "CALIBRATION"]
TYPE_WEIGHTS = [30, 35, 20, 15]
STATUSES = ["OPEN", "IN_PROGRESS", "CLOSED", "CANCELLED"]
STATUS_WEIGHTS = [15, 10, 65, 10]
REQUESTORS = [
    "AutoScheduler", "J. Martinez", "K. Chen", "A. Patel",
    "M. Johnson", "S. Lee", "R. Garcia", "T. Williams",
    "FleetMonitor", "PredictiveEngine", "L. Brown", "D. Kim",
]

DESCRIPTIONS = [
    "Scheduled preventive maintenance per OEM guidelines",
    "Vibration levels exceeded threshold - immediate inspection required",
    "Temperature anomaly detected during production run",
    "Quarterly calibration of pressure sensors",
    "Bearing replacement due to wear pattern detected",
    "Lubrication system check and fluid replacement",
    "Electrical system inspection after power fluctuation",
    "Motor alignment verification after vibration alert",
    "Coolant flow rate below minimum specification",
    "Safety interlock system validation",
    "Belt tension adjustment needed per diagnostic",
    "Hydraulic pressure drop detected - seal inspection",
    "Annual compliance inspection",
    "Firmware update for control module",
    "Filter replacement based on differential pressure reading",
    "Gearbox oil analysis showed elevated metal particles",
    "Production quality deviation traced to machine parameters",
    "Emergency shutdown investigation and restart procedure",
    "Thermal imaging revealed hotspot on terminal block",
    "Operator reported unusual noise during operation",
]

TARGET_ROWS = 1_000_000
BATCH_SIZE = 100_000


def generate_batch(n: int, now: datetime) -> io.StringIO:
    buf = io.StringIO()
    ninety_days_ago = now - timedelta(days=90)
    span_seconds = int((now - ninety_days_ago).total_seconds())

    for _ in range(n):
        rid = str(uuid.uuid4())
        machine_id = random.choice(MACHINE_IDS)
        priority = random.choices(PRIORITIES, weights=PRIORITY_WEIGHTS, k=1)[0]
        req_type = random.choices(REQUEST_TYPES, weights=TYPE_WEIGHTS, k=1)[0]
        description = random.choice(DESCRIPTIONS)
        requestor = random.choice(REQUESTORS)
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

        offset_secs = random.randint(0, span_seconds)
        created_at = ninety_days_ago + timedelta(seconds=offset_secs)
        if status in ("CLOSED", "CANCELLED"):
            updated_at = created_at + timedelta(
                hours=random.randint(1, 72),
                minutes=random.randint(0, 59),
            )
        elif status == "IN_PROGRESS":
            updated_at = created_at + timedelta(
                minutes=random.randint(5, 120),
            )
        else:
            updated_at = created_at

        buf.write(
            f"{rid}\t{machine_id}\t{priority}\t{req_type}\t"
            f"{description}\t{requestor}\t{status}\t"
            f"{created_at.isoformat()}\t{updated_at.isoformat()}\n"
        )
    buf.seek(0)
    return buf


def main():
    w = WorkspaceClient()
    instance = w.database.get_database_instance(name=INSTANCE_NAME)
    host = instance.read_write_dns
    user = w.current_user.me().user_name

    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[INSTANCE_NAME],
    )

    conn = psycopg.connect(
        host=host,
        port=5432,
        dbname=DB_NAME,
        user=user,
        password=cred.token,
        sslmode="require",
        connect_timeout=15,
    )

    now = datetime.now(timezone.utc)
    total_inserted = 0
    start = time.time()

    print(f"Inserting {TARGET_ROWS:,} service request records...")
    columns = (
        "id", "machine_id", "priority", "request_type",
        "description", "requestor", "status", "created_at", "updated_at",
    )
    copy_sql = f"COPY service_requests ({','.join(columns)}) FROM STDIN"

    try:
        for batch_num in range(TARGET_ROWS // BATCH_SIZE):
            batch_start = time.time()
            buf = generate_batch(BATCH_SIZE, now)

            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    while data := buf.read(65536):
                        copy.write(data)
            conn.commit()

            total_inserted += BATCH_SIZE
            elapsed = time.time() - batch_start
            total_elapsed = time.time() - start
            rate = total_inserted / total_elapsed
            print(
                f"  Batch {batch_num + 1}: {BATCH_SIZE:,} rows in {elapsed:.1f}s "
                f"| Total: {total_inserted:,} | Rate: {rate:,.0f} rows/sec"
            )
    finally:
        conn.close()

    total_time = time.time() - start
    print(f"\nDone! Inserted {total_inserted:,} rows in {total_time:.1f}s "
          f"({total_inserted/total_time:,.0f} rows/sec)")


if __name__ == "__main__":
    main()
