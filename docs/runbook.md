# Runbook

These commands preserve the container names and internal paths from the original
local notes. Run them from the repository root in PowerShell, and adjust them
if your containers use different names or paths.

## 1. Start the Flink ingestion job

```powershell
docker cp .\sql\flink\01_ingest_transactions.sql flink-jobmanager:/opt/flink/01_ingest_transactions.sql
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/01_ingest_transactions.sql
```

Keep the SQL client/job running to consume the Kafka topic.

## 2. Refresh the one-minute game metrics

Run after the Flink job has written events to the Iceberg silver table:

```powershell
docker cp .\sql\spark\02_game_metrics_1m.sql spark-iceberg:/opt/spark/02_game_metrics_1m.sql
docker exec spark-iceberg spark-sql --master spark://spark-iceberg:7077 -f /opt/spark/02_game_metrics_1m.sql
```

This is a batch aggregation over the current silver-table contents, not a
continuous Spark streaming job.

## 3. Run transaction reconciliation

```powershell
docker cp .\sql\spark\03_transaction_reconciliation.sql spark-iceberg:/opt/spark/03_transaction_reconciliation.sql
docker exec spark-iceberg spark-sql --master spark://spark-iceberg:7077 -f /opt/spark/03_transaction_reconciliation.sql
```

The reconciliation script appends findings on every run. Clear or otherwise
manage the findings table deliberately if you need a fresh reconciliation
snapshot; do not assume repeated runs are idempotent.

## 4. Inspect tables and run read-only checks

```powershell
docker cp .\sql\ops\04_diagnostics.sql spark-iceberg:/opt/spark/04_diagnostics.sql
docker exec spark-iceberg spark-sql --master spark://spark-iceberg:7077 -f /opt/spark/04_diagnostics.sql
```

The diagnostics script contains queries only; it does not delete or modify
pipeline data.