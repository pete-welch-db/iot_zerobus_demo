"""
Create or refresh Unity Catalog metric views for IoT demo KPIs.
"""

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create UC metric views for IoT demo.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    schema = args.schema

    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"USE SCHEMA {schema}")

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.mv_machine_telemetry
        WITH METRICS
        LANGUAGE YAML
        AS $$
          version: 1.1
          source: {catalog}.{schema}.silver_machine_telemetry
          dimensions:
            - name: Machine
              expr: machine_id
            - name: State
              expr: state
            - name: Event Time
              expr: event_time
            - name: Event Date
              expr: DATE(event_time)
            - name: Event Hour
              expr: DATE_TRUNC('HOUR', event_time)
          measures:
            - name: Event Count
              expr: COUNT(1)
            - name: Avg Temperature C
              expr: AVG(temp_c)
            - name: Avg Vibration mm/s
              expr: AVG(vibration_mm_s)
            - name: Avg Throughput CPM
              expr: AVG(throughput_cpm)
            - name: Avg Load Pct
              expr: AVG(load_pct)
            - name: Avg Power kW
              expr: AVG(power_kw)
            - name: Avg Pressure bar
              expr: AVG(pressure_bar)
            - name: Avg Flow LPM
              expr: AVG(flow_rate_lpm)
        $$
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.mv_machine_oee
        WITH METRICS
        LANGUAGE YAML
        AS $$
          version: 1.1
          source: {catalog}.{schema}.gold_machine_health_5m
          dimensions:
            - name: Machine
              expr: machine_id
            - name: Window End
              expr: window_end
            - name: Window Date
              expr: DATE(window_end)
            - name: Window Hour
              expr: DATE_TRUNC('HOUR', window_end)
          measures:
            - name: OEE Pct
              expr: AVG(oee_pct)
            - name: Availability Pct
              expr: AVG(availability_pct)
            - name: Performance Pct
              expr: AVG(performance_pct)
            - name: Quality Pct
              expr: AVG(quality_pct)
        $$
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.mv_machine_downtime
        WITH METRICS
        LANGUAGE YAML
        AS $$
          version: 1.1
          source: {catalog}.{schema}.gold_machine_health_5m
          dimensions:
            - name: Machine
              expr: machine_id
            - name: Window End
              expr: window_end
            - name: Window Date
              expr: DATE(window_end)
          measures:
            - name: Run Seconds
              expr: SUM(time_in_run_s)
            - name: Stopped Seconds
              expr: SUM(time_in_stopped_s)
            - name: Fault Seconds
              expr: SUM(time_in_fault_s)
            - name: Downtime Seconds
              expr: SUM(time_in_stopped_s + time_in_fault_s)
            - name: Downtime Pct
              expr: SUM(time_in_stopped_s + time_in_fault_s) / NULLIF(SUM(time_in_run_s + time_in_stopped_s + time_in_fault_s), 0) * 100
        $$
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.mv_machine_risk
        WITH METRICS
        LANGUAGE YAML
        AS $$
          version: 1.1
          source: {catalog}.{schema}.vw_machine_health
          dimensions:
            - name: Machine
              expr: machine_id
            - name: Window End
              expr: window_end
            - name: Window Date
              expr: DATE(window_end)
          measures:
            - name: Avg Anomaly Score
              expr: AVG(anomaly_score)
            - name: Avg Fault Risk 5m
              expr: AVG(prob_fault_next_5m)
            - name: Avg Fault Risk 1h
              expr: AVG(prob_fault_next_1h)
            - name: Avg Fault Risk 24h
              expr: AVG(prob_fault_next_24h)
            - name: Avg Fault Risk 7d
              expr: AVG(prob_fault_next_7d)
            - name: High Risk Windows
              expr: SUM(CASE WHEN prob_fault_next_5m >= 0.5 THEN 1 ELSE 0 END)
            - name: Anomaly Windows
              expr: SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)
        $$
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.mv_machine_freshness
        WITH METRICS
        LANGUAGE YAML
        AS $$
          version: 1.1
          source: {catalog}.{schema}.vw_machine_current_status
          dimensions:
            - name: Machine
              expr: machine_id
            - name: State
              expr: state
            - name: Last Event Time
              expr: last_event_time
          measures:
            - name: Avg Telemetry Lag Seconds
              expr: AVG(telemetry_lag_seconds)
            - name: Avg ML Lag Seconds
              expr: AVG(ml_lag_seconds)
            - name: Max Telemetry Lag Seconds
              expr: MAX(telemetry_lag_seconds)
            - name: Max ML Lag Seconds
              expr: MAX(ml_lag_seconds)
            - name: Avg Telemetry Lag Ms
              expr: AVG(telemetry_lag_ms)
            - name: Avg ML Lag Ms
              expr: AVG(ml_lag_ms)
            - name: Max Telemetry Lag Ms
              expr: MAX(telemetry_lag_ms)
            - name: Max ML Lag Ms
              expr: MAX(ml_lag_ms)
        $$
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.mv_machine_current
        WITH METRICS
        LANGUAGE YAML
        AS $$
          version: 1.1
          source: {catalog}.{schema}.vw_machine_current_status
          dimensions:
            - name: Machine
              expr: machine_id
            - name: State
              expr: state
            - name: Last Event Time
              expr: last_event_time
          measures:
            - name: Current Temperature C
              expr: AVG(temp_c)
            - name: Current Vibration mm/s
              expr: AVG(vibration_mm_s)
            - name: Current Throughput CPM
              expr: AVG(throughput_cpm)
            - name: Current Load Pct
              expr: AVG(load_pct)
            - name: Current OEE Pct
              expr: AVG(oee_pct)
            - name: Current Fault Risk 5m
              expr: AVG(prob_fault_next_5m)
            - name: Current Fault Risk 1h
              expr: AVG(prob_fault_next_1h)
            - name: Current Fault Risk 24h
              expr: AVG(prob_fault_next_24h)
            - name: Current Fault Risk 7d
              expr: AVG(prob_fault_next_7d)
            - name: Current Anomaly Score
              expr: AVG(anomaly_score)
            - name: Current Power kW
              expr: AVG(power_kw)
            - name: Current Pressure bar
              expr: AVG(pressure_bar)
            - name: Current Flow LPM
              expr: AVG(flow_rate_lpm)
        $$
        """
    )

    print(f"UC metric views refreshed in {catalog}.{schema}:")
    print("- mv_machine_telemetry")
    print("- mv_machine_oee")
    print("- mv_machine_downtime")
    print("- mv_machine_risk")
    print("- mv_machine_freshness")
    print("- mv_machine_current")


if __name__ == "__main__":
    main()
