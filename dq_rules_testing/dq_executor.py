"""
PySpark Data Quality Queue Executor
====================================
Reads DQ rules from Oracle EONE.EIDF_DQ_QUERY, executes them sequentially
via Spark SQL, and writes results back to EONE.EIDF_DQ_RESULTS.
"""

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DQ_QUERY_TABLE = "EONE.EIDF_DQ_QUERY"
DQ_RESULTS_TABLE = "EONE.EIDF_DQ_RESULTS"

PLACEHOLDER_PATTERN = re.compile(r"\$[a-zA-Z_]+")

EXPECTED_RESULT_COLUMNS = frozenset(
    ["SOR", "TOTAL_COUNT", "TOTAL_DOLLAR_AMOUNT", "FAIL_COUNT", "FAIL_DOLLAR_AMOUNT"]
)

RESULTS_SCHEMA = StructType(
    [
        StructField("DQ_ID", LongType(), False),
        StructField("DQ_CHK_ID", StringType(), False),
        StructField("DQ_QUEUE_ID", LongType(), False),
        StructField("EXECUTION_ID", StringType(), False),
        StructField("RUN_FIC_MIS_DATE", StringType(), True),
        StructField("RUN_AS_OF_DT", StringType(), True),
        StructField("RUN_DMN", StringType(), True),
        StructField("SOR", StringType(), False),
        StructField("TOTAL_COUNT", LongType(), True),
        StructField("TOTAL_DOLLAR_AMOUNT", LongType(), True),
        StructField("FAIL_COUNT", LongType(), True),
        StructField("FAIL_DOLLAR_AMOUNT", LongType(), True),
        StructField("FAIL_COUNT_PERCENTAGE", DecimalType(5, 2), True),
        StructField("FAIL_DOLLAR_PERCENTAGE", DecimalType(5, 2), True),
        StructField("EXECUTION_STATUS", StringType(), True),
        StructField("ERROR_MESSAGE", StringType(), True),
        StructField("EXECUTION_START_DT", TimestampType(), True),
        StructField("EXECUTION_END_DT", TimestampType(), True),
        StructField("CREATED_DT", TimestampType(), True),
    ]
)

logger = logging.getLogger("dq_executor")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_jdbc_url(cfg: Dict[str, str]) -> str:
    return (
        f"jdbc:oracle:thin:@//{cfg['host']}:{cfg['port']}/{cfg['service_name']}"
    )


def _jdbc_properties(cfg: Dict[str, str], password: str) -> Dict[str, str]:
    return {
        "user": cfg["user"],
        "password": password,
        "driver": "oracle.jdbc.OracleDriver",
        "fetchsize": "10000",
    }


def _now() -> datetime:
    return datetime.now()


# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------
def load_rules(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: Dict[str, str],
    queue_id: int,
    domain: str,
) -> DataFrame:
    """Load active DQ rules from Oracle for the given queue and domain."""

    query = (
        f"(SELECT * FROM {DQ_QUERY_TABLE} "
        f"WHERE DQ_QUEUE_ID = {queue_id} "
        f"AND DQ_LRI = 'Y' "
        f"AND DQ_DMN = '{domain}') dq_rules"
    )

    logger.info("Loading rules: queue_id=%s, domain=%s", queue_id, domain)

    dq_rules_df = (
        spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props)
    )

    rule_count = dq_rules_df.count()
    logger.info("Loaded %d active rule(s)", rule_count)

    if rule_count == 0:
        logger.warning("No active rules found — exiting gracefully.")
        spark.stop()
        sys.exit(0)

    return dq_rules_df


def replace_placeholders(
    sql: str,
    schema: Optional[str],
    sor: Optional[str],
    fic_mis_date: str,
    as_of_dt: str,
) -> str:
    """Replace known placeholders and validate no unresolved variables remain."""

    replacements = {
        "$ds": schema or "",
        "$vdo": sor or "",
        "$misdate": fic_mis_date,
        "$asofdt": as_of_dt,
    }

    final_sql = sql
    for placeholder, value in replacements.items():
        final_sql = final_sql.replace(placeholder, value)

    remaining = PLACEHOLDER_PATTERN.findall(final_sql)
    if remaining:
        raise ValueError(
            f"Unresolved placeholders in SQL: {remaining}. "
            f"Final SQL: {final_sql[:500]}"
        )

    return final_sql


def execute_rule(
    spark: SparkSession,
    rule: Dict[str, Any],
    execution_id: str,
    fic_mis_date: str,
    as_of_dt: str,
    domain: str,
) -> Dict[str, Any]:
    """Execute a single DQ rule and return a result dict."""

    dq_id = rule["DQ_ID"]
    dq_chk_id = rule["DQ_CHK_ID"]
    dq_queue_id = rule["DQ_QUEUE_ID"]
    dq_sql = rule["DQ_SQL"]
    dq_schema = rule.get("DQ_SCHEMA")
    dq_sor = rule.get("DQ_SOR")

    logger.info(
        "Executing rule DQ_ID=%s  DQ_CHK_ID=%s", dq_id, dq_chk_id
    )

    exec_start = _now()

    # Base metadata shared by success and failure paths
    base = {
        "DQ_ID": int(dq_id),
        "DQ_CHK_ID": str(dq_chk_id),
        "DQ_QUEUE_ID": int(dq_queue_id),
        "EXECUTION_ID": execution_id,
        "RUN_FIC_MIS_DATE": fic_mis_date,
        "RUN_AS_OF_DT": as_of_dt,
        "RUN_DMN": domain,
        "EXECUTION_START_DT": exec_start,
    }

    try:
        # Placeholder replacement
        final_sql = replace_placeholders(
            sql=dq_sql,
            schema=dq_schema,
            sor=dq_sor,
            fic_mis_date=fic_mis_date,
            as_of_dt=as_of_dt,
        )

        logger.debug("Final SQL (DQ_ID=%s): %s", dq_id, final_sql[:300])

        # Execute via Spark SQL
        result_df = spark.sql(final_sql)

        # Validate expected columns
        result_cols_upper = {c.upper() for c in result_df.columns}
        missing = EXPECTED_RESULT_COLUMNS - result_cols_upper
        if missing:
            raise ValueError(
                f"Result is missing required columns: {missing}. "
                f"Got: {result_df.columns}"
            )

        # Normalise column names to upper-case
        for col in result_df.columns:
            result_df = result_df.withColumnRenamed(col, col.upper())

        # Collect result — DQ SQL should return a small summary row
        rows = result_df.collect()
        if not rows:
            raise ValueError("DQ SQL returned zero rows.")

        exec_end = _now()
        results: List[Dict[str, Any]] = []

        for row in rows:
            total_count = int(row["TOTAL_COUNT"] or 0)
            total_dollar = int(row["TOTAL_DOLLAR_AMOUNT"] or 0)
            fail_count = int(row["FAIL_COUNT"] or 0)
            fail_dollar = int(row["FAIL_DOLLAR_AMOUNT"] or 0)

            fail_count_pct = round(
                (fail_count / total_count * 100) if total_count > 0 else 0, 2
            )
            fail_dollar_pct = round(
                (fail_dollar / total_dollar * 100) if total_dollar > 0 else 0, 2
            )

            results.append(
                {
                    **base,
                    "SOR": str(row["SOR"] or "NA"),
                    "TOTAL_COUNT": total_count,
                    "TOTAL_DOLLAR_AMOUNT": total_dollar,
                    "FAIL_COUNT": fail_count,
                    "FAIL_DOLLAR_AMOUNT": fail_dollar,
                    "FAIL_COUNT_PERCENTAGE": fail_count_pct,
                    "FAIL_DOLLAR_PERCENTAGE": fail_dollar_pct,
                    "EXECUTION_STATUS": "SUCCESS",
                    "ERROR_MESSAGE": None,
                    "EXECUTION_END_DT": exec_end,
                    "CREATED_DT": exec_end,
                }
            )

        elapsed_ms = int((exec_end - exec_start).total_seconds() * 1000)
        logger.info(
            "Rule DQ_ID=%s completed: %d result row(s), %d ms",
            dq_id,
            len(results),
            elapsed_ms,
        )
        return {"status": "SUCCESS", "rows": results}

    except Exception as exc:
        exec_end = _now()
        error_msg = str(exc)[:4000]
        logger.error("Rule DQ_ID=%s FAILED: %s", dq_id, error_msg)

        failure_row = {
            **base,
            "SOR": "NA",
            "TOTAL_COUNT": 0,
            "TOTAL_DOLLAR_AMOUNT": 0,
            "FAIL_COUNT": 0,
            "FAIL_DOLLAR_AMOUNT": 0,
            "FAIL_COUNT_PERCENTAGE": 0.0,
            "FAIL_DOLLAR_PERCENTAGE": 0.0,
            "EXECUTION_STATUS": "FAILED",
            "ERROR_MESSAGE": error_msg,
            "EXECUTION_END_DT": exec_end,
            "CREATED_DT": exec_end,
        }
        return {"status": "FAILED", "rows": [failure_row]}


def write_results(
    spark: SparkSession,
    results: List[Dict[str, Any]],
    jdbc_url: str,
    jdbc_props: Dict[str, str],
) -> None:
    """Write collected result rows to Oracle EIDF_DQ_RESULTS via JDBC."""

    if not results:
        logger.warning("No results to write.")
        return

    results_df = spark.createDataFrame(results, schema=RESULTS_SCHEMA)

    logger.info("Writing %d result row(s) to %s", results_df.count(), DQ_RESULTS_TABLE)

    (
        results_df.write.jdbc(
            url=jdbc_url,
            table=DQ_RESULTS_TABLE,
            mode="append",
            properties={
                **jdbc_props,
                "batchsize": "1000",
                "isolationLevel": "NONE",
            },
        )
    )

    logger.info("Results written successfully.")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run(args: argparse.Namespace) -> None:
    """Main orchestration: load → replace → execute → write."""

    # --- Oracle config ---
    oracle_cfg = _parse_oracle_config(args.oracle_config)
    jdbc_url = _build_jdbc_url(oracle_cfg)
    jdbc_props = _jdbc_properties(oracle_cfg, args.oracle_password)

    execution_id = args.execution_id or str(uuid.uuid4())

    logger.info("=" * 60)
    logger.info("DQ Queue Executor — START")
    logger.info("Execution ID : %s", execution_id)
    logger.info("Queue ID     : %s", args.dq_queue_id)
    logger.info("Domain       : %s", args.dq_domain)
    logger.info("FIC MIS Date : %s", args.fic_mis_date)
    logger.info("As-Of Date   : %s", args.as_of_dt)
    logger.info("=" * 60)

    # --- Spark session ---
    spark = _create_spark_session()

    try:
        # Step 1: Load rules
        dq_rules_df = load_rules(
            spark, jdbc_url, jdbc_props, args.dq_queue_id, args.dq_domain
        )

        # Collect rule metadata (small dataset) ordered by DQ_ID
        rules = (
            dq_rules_df.orderBy(F.col("DQ_ID").asc()).collect()
        )
        rule_dicts = [row.asDict() for row in rules]

        logger.info("Executing %d rule(s) sequentially …", len(rule_dicts))

        # Step 2 & 3: Execute each rule sequentially
        all_results: List[Dict[str, Any]] = []
        success_count = 0
        fail_count = 0

        for idx, rule in enumerate(rule_dicts, start=1):
            logger.info(
                "--- Rule %d/%d (DQ_ID=%s) ---", idx, len(rule_dicts), rule["DQ_ID"]
            )
            outcome = execute_rule(
                spark=spark,
                rule=rule,
                execution_id=execution_id,
                fic_mis_date=args.fic_mis_date,
                as_of_dt=args.as_of_dt,
                domain=args.dq_domain,
            )
            all_results.extend(outcome["rows"])

            if outcome["status"] == "SUCCESS":
                success_count += 1
            else:
                fail_count += 1

        logger.info(
            "Execution complete: %d succeeded, %d failed out of %d rule(s)",
            success_count,
            fail_count,
            len(rule_dicts),
        )

        # Step 4: Write results to Oracle
        write_results(spark, all_results, jdbc_url, jdbc_props)

    except Exception:
        logger.exception("Queue execution failed with an unrecoverable error.")
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")

    logger.info("DQ Queue Executor — DONE")


# ---------------------------------------------------------------------------
# Spark & Config Helpers
# ---------------------------------------------------------------------------
def _create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DQ_Queue_Executor")
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def _parse_oracle_config(value: str) -> Dict[str, str]:
    """Accept a JSON string or a file path pointing to a JSON file."""
    if os.path.isfile(value):
        with open(value, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    else:
        cfg = json.loads(value)

    required_keys = {"host", "port", "service_name", "user"}
    missing = required_keys - set(cfg.keys())
    if missing:
        raise ValueError(f"Oracle config missing keys: {missing}")

    return cfg


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PySpark DQ Queue Executor — runs data-quality rules from Oracle."
    )

    parser.add_argument(
        "--oracle-config",
        required=True,
        help="Oracle connection config as JSON string or path to JSON file.",
    )
    parser.add_argument(
        "--oracle-password",
        required=True,
        help="Oracle password.",
    )
    parser.add_argument(
        "--dq-queue-id",
        type=int,
        required=True,
        help="DQ_QUEUE_ID to execute.",
    )
    parser.add_argument(
        "--dq-domain",
        required=True,
        choices=["FMR", "EIDF"],
        help="Domain filter (FMR or EIDF).",
    )
    parser.add_argument(
        "--fic-mis-date",
        required=True,
        help="FIC MIS date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--as-of-dt",
        required=True,
        help="As-of date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--execution-id",
        default=None,
        help="Execution ID (auto-generated UUID if not provided).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )

    return parser.parse_args(argv)


def _configure_logging(level: str) -> None:
    log_format = (
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Silence noisy Spark/Py4J loggers
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def main() -> None:
    args = _parse_args()
    _configure_logging(args.log_level)
    run(args)


if __name__ == "__main__":
    main()
