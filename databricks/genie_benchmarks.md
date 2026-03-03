# Genie Space Benchmarks

Add these benchmark questions manually via the Genie Space UI (Benchmarks > Add benchmark).
Use 2-4 phrasings per logical question with the same SQL answer for accuracy assessment.

Replace `{catalog}` and `{schema}` with your catalog and schema (e.g. welch, iot_demo_dev).

## Benchmark Questions with SQL Answers

### Which machine is most likely to fault?
**Phrasings:** "Which machine is most likely to fault in the next 5 minutes?" / "Which machine has the highest fault risk?" / "Which machine is at highest risk of failing?"
```sql
SELECT machine_id, prob_fault_next_5m, anomaly_score
FROM {catalog}.{schema}.vw_machine_current_status
ORDER BY prob_fault_next_5m DESC
LIMIT 5;
```

### Latest OEE by machine
**Phrasings:** "What is the latest OEE for each machine?" / "Show OEE by machine"
```sql
SELECT machine_id, oee_pct, availability_pct, performance_pct, quality_pct
FROM {catalog}.{schema}.vw_machine_current_status;
```

### Telemetry lag by machine
**Phrasings:** "Which machine has the highest telemetry lag?" / "Show telemetry lag by machine"
```sql
SELECT machine_id, telemetry_lag_seconds, ml_lag_seconds, last_event_time
FROM {catalog}.{schema}.vw_machine_current_status
ORDER BY telemetry_lag_seconds DESC;
```

### Downtime by machine
**Phrasings:** "Show downtime for each machine" / "What is the downtime by machine in the last hour?"
```sql
SELECT machine_id, SUM(time_in_stopped_s + time_in_fault_s) AS downtime_s
FROM {catalog}.{schema}.vw_machine_health
WHERE window_end >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY machine_id;
```

### Machines with anomaly above threshold
**Phrasings:** "Which machines have anomaly score above 0.7?" / "Which machines are anomalous?"
```sql
SELECT machine_id, anomaly_score, prob_fault_next_5m, state
FROM {catalog}.{schema}.vw_machine_current_status
WHERE anomaly_score >= 0.7;
```

### Average telemetry lag for MACH_A
**Phrasings:** "What is the average telemetry lag for MACH_A?" / "How fresh is MACH_A data?"
```sql
SELECT machine_id, telemetry_lag_seconds, ml_lag_seconds
FROM {catalog}.{schema}.vw_machine_current_status
WHERE machine_id = 'MACH_A';
```

### Compare OEE across lines
**Phrasings:** "Compare OEE across all lines" / "What is OEE by line for the latest window?"
```sql
SELECT s.machine_id, d.line_name, s.oee_pct, s.availability_pct, s.performance_pct, s.quality_pct
FROM {catalog}.{schema}.vw_machine_current_status s
LEFT JOIN {catalog}.{schema}.dim_machine d ON s.machine_id = d.machine_id;
```
