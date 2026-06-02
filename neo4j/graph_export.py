# scripts/graph_export.py

import logging
import os
from datetime import datetime

from neo4j import GraphDatabase
from pyspark.sql import SparkSession


# =========================================================
# CONFIG
# =========================================================

GRAPH_INPUT_PATH = "data/curated/graph_edges"

NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "<password>")

BATCH_SIZE = 1000


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =========================================================
# SPARK SESSION
# =========================================================

def create_spark():
    return (
        SparkSession.builder
        .appName("OmniRisk-GraphExport")
        .getOrCreate()
    )


# =========================================================
# LOAD DATA FROM SPARK OUTPUT
# =========================================================

def load_edges(spark):

    logger.info("Loading graph edges from Spark output...")

    return spark.read.parquet(GRAPH_INPUT_PATH)


# =========================================================
# NEO4J LOADER
# =========================================================

class Neo4jLoader:

    def __init__(self):

        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def close(self):
        self.driver.close()

    # -----------------------------------------------------
    # Bulk load
    # -----------------------------------------------------

    def load_edges(self, spark_df):

        logger.info("Loading data into Neo4j...")

        query = """
        UNWIND $rows AS row

        MERGE (a:Account {id: row.src})
        MERGE (b:Account {id: row.tgt})

        MERGE (a)-[r:GUARANTEE]->(b)

        SET r.day = row.day,
            r.amount = row.amount
        """

        total = 0

        with self.driver.session() as session:

            batch = []

            for row in spark_df.toLocalIterator():

                batch.append({
                    "src": row["source_account_id"],
                    "tgt": row["target_account_id"],
                    "amount": float(row["guarantee_amount_usd"]),
                    "day": str(row["transaction_day"])
                })

                if len(batch) >= BATCH_SIZE:

                    session.run(query, rows=batch)

                    total += len(batch)

                    logger.info(
                        f"Loaded {total:,} relationships..."
                    )

                    batch = []

            if batch:

                session.run(query, rows=batch)

                total += len(batch)

        logger.info(
            f"Finished Neo4j load. Total edges: {total:,}"
        )

    # -----------------------------------------------------
    # Fraud cycles
    # -----------------------------------------------------

    def detect_cycles(self):

        logger.info(
            "Detecting cycles directly in Neo4j..."
        )

        query = """
        MATCH p=(a:Account)-[:GUARANTEE*2..3]->(a)
        RETURN count(p) AS cycle_count
        """

        with self.driver.session() as session:

            result = session.run(query)

            record = result.single()

            count = record["cycle_count"]

        logger.info(
            f"Cycles found in Neo4j: {count}"
        )

        return count


# =========================================================
# CYCLICAL FRAUD QUERY (REQ CHALLENGE)
# =========================================================

def print_cypher_query():

    logger.info(
        "Cypher query for fraud cycles (3 levels):"
    )

    query = """
    MATCH p=(a:Account)-[:GUARANTEE*1..3]->(a)
    RETURN p
    """

    print(query)


# =========================================================
# MAIN
# =========================================================

def main():

    start = datetime.utcnow()

    spark = create_spark()

    try:

        # ---------------------------------------------
        # Load graph edges from parquet
        # ---------------------------------------------

        df = load_edges(spark)

        logger.info(
            f"Edges available: {df.count():,}"
        )

        # ---------------------------------------------
        # Export to Neo4j
        # ---------------------------------------------

        neo4j = Neo4jLoader()

        neo4j.load_edges(df)

        # ---------------------------------------------
        # Detect fraud cycles in Neo4j
        # ---------------------------------------------

        cycles_detected = neo4j.detect_cycles()

        neo4j.close()

        # ---------------------------------------------
        # Print challenge query
        # ---------------------------------------------

        print_cypher_query()

        logger.info({
            "event": "graph_export_completed",
            "cycles_detected": cycles_detected,
            "execution_time_seconds":
                (datetime.utcnow() - start).total_seconds()
        })

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
