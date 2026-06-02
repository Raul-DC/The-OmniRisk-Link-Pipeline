# scripts/process_transactions.py

import argparse
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    to_date,
    sum as spark_sum,
    count,
    avg,
    when,
    current_timestamp,
    round as spark_round,
    lit
)
from pyspark.storagelevel import StorageLevel


# =========================================================
# BASE PATH (DOCKER COMPATIBLE)
# =========================================================

TRANSACTIONS_PATH = "/workspace/data/transactions"
FREE_ACCOUNTS_PATH = "/workspace/data/free_accounts.csv.gz"
OUTPUT_PATH = "/workspace/data/curated/transactions"
RISK_OUTPUT_PATH = "/workspace/data/curated/risk_metrics"
SUSPICIOUS_OUTPUT_PATH = "/workspace/data/curated/high_risk_accounts"
GRAPH_OUTPUT_PATH = "/workspace/data/curated/graph_edges"


VALID_COUNTRIES = {"CL", "PE", "MX", "BR"}
FREE_BENEFITS = {"FREE_PROMO_90D", "PARTNER_ZERO_FEE", "WELCOME_OFFER"}


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("omnirisk-spark")


# =========================================================
# ARGUMENTS
# =========================================================

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-date", required=False)
    return parser.parse_args()


# =========================================================
# SPARK SESSION
# =========================================================

def create_spark_session():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("OmniRisk-Transactions")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# =========================================================
# LOAD TRANSACTIONS
# =========================================================

def load_transactions(spark, process_date=None):

    logger.info("Loading transactions...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{TRANSACTIONS_PATH}/day=*/transactions.csv")
        .withColumn("transaction_day", to_date(col("timestamp")))
    )

    if process_date:
        end_date = datetime.strptime(process_date, "%Y-%m-%d")
        start_date = end_date - timedelta(days=6)

        logger.info(
            f"Filtering incremental window: "
            f"{start_date.date()} → {end_date.date()}"
        )

        df = df.filter(
            col("transaction_day").between(
                lit(start_date.date()),
                lit(end_date.date())
            )
        )

    return df


# =========================================================
# LOAD FREE ACCOUNTS
# =========================================================

def load_free_accounts(spark):

    return (
        spark.read
        .option("header", True)
        .csv(FREE_ACCOUNTS_PATH)
    )


# =========================================================
# PIPELINE
# =========================================================

def validate_data_quality(df):
    return (
        df.filter(col("transaction_id").isNotNull())
        .dropDuplicates(["transaction_id"])
        .filter(col("guarantee_amount_usd").isNotNull())
        .filter(col("fee_amount_usd").isNotNull())
        .filter(col("guarantee_amount_usd") > 0)
        .filter(col("fee_amount_usd") >= 0)
        .filter(col("country_code").isin(list(VALID_COUNTRIES)))
    )


def filter_approved_transactions(df):
    return df.filter(col("status") == "APPROVED")


def exclude_free_accounts(transactions_df, free_accounts_df):

    free_accounts_filtered = (
        free_accounts_df
        .filter(col("benefit_type").isin(list(FREE_BENEFITS)))
        .select("account_id")
        .distinct()
    )

    return (
        transactions_df
        .join(
            broadcast(free_accounts_filtered),
            transactions_df.source_account_id == free_accounts_filtered.account_id,
            "left_anti"
        )
        .join(
            broadcast(free_accounts_filtered),
            transactions_df.target_account_id == free_accounts_filtered.account_id,
            "left_anti"
        )
    )


def enrich_transactions(df):

    safe_amount = when(
        col("guarantee_amount_usd") == 0,
        lit(None)
    ).otherwise(col("guarantee_amount_usd"))

    return (
        df
        .withColumn("high_risk_transaction",
                    when(col("guarantee_amount_usd") > 50000, True).otherwise(False))
        .withColumn("very_high_risk_transaction",
                    when(col("guarantee_amount_usd") > 200000, True).otherwise(False))
        .withColumn("fee_percentage",
                    when(safe_amount.isNotNull(),
                         spark_round((col("fee_amount_usd") / safe_amount) * 100, 2)))
        .withColumn("risk_category",
                    when(col("guarantee_amount_usd") > 200000, "CRITICAL")
                    .when(col("guarantee_amount_usd") > 50000, "HIGH")
                    .otherwise("NORMAL"))
        .withColumn("processed_at", current_timestamp())
    )


# =========================================================
# GRAPH
# =========================================================

def prepare_graph_layer(df):
    return df.select(
        "source_account_id",
        "target_account_id",
        "guarantee_amount_usd",
        "transaction_day"
    )


def save_graph_layer(df):
    logger.info("Saving graph layer...")

    df.repartition("transaction_day") \
        .write \
        .mode("overwrite") \
        .partitionBy("transaction_day") \
        .parquet(GRAPH_OUTPUT_PATH)


# =========================================================
# METRICS
# =========================================================

def generate_risk_metrics(df):
    return (
        df.groupBy("transaction_day")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum("guarantee_amount_usd").alias("total_guarantee_amount"),
            avg("guarantee_amount_usd").alias("avg_transaction_amount"),
            spark_sum(
                when(col("high_risk_transaction"), 1).otherwise(0)
            ).alias("high_risk_transactions")
        )
    )


def detect_high_volume_accounts(df):
    return (
        df.groupBy("source_account_id")
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("guarantee_amount_usd").alias("total_amount")
        )
        .filter(col("transaction_count") > 10)
    )


# =========================================================
# SAVE
# =========================================================

def save_curated_data(df):
    df.repartition("transaction_day") \
        .write \
        .mode("overwrite") \
        .partitionBy("transaction_day") \
        .parquet(OUTPUT_PATH)


def save_risk_metrics(df):
    df.write.mode("overwrite").parquet(RISK_OUTPUT_PATH)


def save_suspicious_accounts(df):
    df.write.mode("overwrite").parquet(SUSPICIOUS_OUTPUT_PATH)


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_args()
    spark = create_spark_session()

    logger.info("Starting OmniRisk Spark Pipeline")

    transactions_df = load_transactions(spark, args.process_date)
    free_accounts_df = load_free_accounts(spark)

    quality_df = validate_data_quality(transactions_df)
    approved_df = filter_approved_transactions(quality_df).persist(StorageLevel.MEMORY_AND_DISK)

    clean_df = exclude_free_accounts(approved_df, free_accounts_df)

    enriched_df = enrich_transactions(clean_df).persist(StorageLevel.MEMORY_AND_DISK)

    save_curated_data(enriched_df)
    save_risk_metrics(generate_risk_metrics(enriched_df))
    save_suspicious_accounts(detect_high_volume_accounts(enriched_df))
    save_graph_layer(prepare_graph_layer(enriched_df))

    approved_df.unpersist()
    enriched_df.unpersist()

    logger.info("Pipeline completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
