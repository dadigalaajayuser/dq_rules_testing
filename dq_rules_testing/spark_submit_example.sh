#!/usr/bin/env bash
# =============================================================================
# spark-submit example for DQ Queue Executor
# =============================================================================
# Prerequisites:
#   - Oracle JDBC driver (ojdbc8.jar) available at the path below
#   - Spark cluster or local mode configured
#   - Oracle connectivity from every executor node
# =============================================================================

ORACLE_JDBC_JAR="/path/to/ojdbc8.jar"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --name "DQ_Queue_Executor" \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 4 \
  --jars "${ORACLE_JDBC_JAR}" \
  --driver-class-path "${ORACLE_JDBC_JAR}" \
  --conf "spark.executor.extraClassPath=${ORACLE_JDBC_JAR}" \
  --conf "spark.sql.adaptive.enabled=true" \
  dq_executor.py \
    --oracle-config '{"host":"exadata-host","port":"1521","service_name":"EONEDB","user":"dq_user"}' \
    --oracle-password "CHANGEME" \
    --dq-queue-id 1 \
    --dq-domain FMR \
    --fic-mis-date "2024-01-31" \
    --as-of-dt "2024-01-31" \
    --log-level INFO

# ---- Alternative: oracle config from a JSON file ----
# spark-submit \
#   ... (same flags) ...
#   dq_executor.py \
#     --oracle-config /etc/dq/oracle_config.json \
#     --oracle-password "${ORACLE_PASSWORD}" \
#     --dq-queue-id 1 \
#     --dq-domain EIDF \
#     --fic-mis-date "2024-01-31" \
#     --as-of-dt "2024-01-31" \
#     --execution-id "run-20240131-001"
