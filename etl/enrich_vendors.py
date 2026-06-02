# scripts/enrich_vendors.py

import os
import json
import time
import random
import hashlib
import logging
import threading
import sys

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests


# =========================================================
# AIRFLOW ARG ({{ ds }})
# =========================================================

execution_date = sys.argv[1] if len(sys.argv) > 1 else None

if execution_date:
    execution_date = execution_date[:10]


# =========================================================
# CONFIGURACION
# =========================================================

INPUT_FILE = "data/sap_vendors.json"

BASE_OUTPUT_DIR = "data/processed/enriched_vendors"

OUTPUT_JSON = "enriched_vendors.json"
OUTPUT_PARQUET = "enriched_vendors.parquet"

MAX_WORKERS = 10
MAX_RETRIES = 5

BASE_BACKOFF = 1
REQUEST_TIMEOUT = 5


# =========================================================
# CIRCUIT BREAKER CONFIG
# =========================================================

CB_FAILURE_THRESHOLD = 5
CB_RECOVERY_TIMEOUT = 15


# =========================================================
# SHA256 SALT (FIX DEMO SAFE)
# =========================================================

# FIX: evita crash en entorno sin variables de entorno (demo-safe)
PII_SALT = os.getenv("PII_SALT", "omnirisk_demo_salt")


# =========================================================
# OUTPUT DIR (IDEMPOTENCIA POR DÍA)
# =========================================================

os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

if execution_date:
    OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, f"day={execution_date}")
else:
    OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "day=unknown")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# =========================================================
# HTTP SESSION
# =========================================================

session = requests.Session()


# =========================================================
# METRICS
# =========================================================

metrics = {
    "success": 0,
    "failed": 0,
    "retries": 0,
    "network_errors": 0
}

metrics_lock = threading.Lock()


# =========================================================
# CIRCUIT BREAKER
# =========================================================

class CircuitBreaker:

    def __init__(self, failure_threshold=5, recovery_timeout=15):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.failure_count = 0
        self.last_failure_time = None

        self.state = "CLOSED"

        self.lock = threading.Lock()
        self.half_open_request_in_progress = False

    def can_execute(self):

        with self.lock:

            if self.state == "CLOSED":
                return True

            if self.state == "OPEN":

                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    self.half_open_request_in_progress = False
                    logging.warning("Circuit Breaker -> HALF_OPEN")
                else:
                    return False

            if self.state == "HALF_OPEN":

                if not self.half_open_request_in_progress:
                    self.half_open_request_in_progress = True
                    return True

                return False

    def record_success(self):
        with self.lock:
            self.failure_count = 0
            self.state = "CLOSED"
            self.half_open_request_in_progress = False

    def record_failure(self):
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"

            if self.state == "HALF_OPEN":
                self.state = "OPEN"

            self.half_open_request_in_progress = False


circuit_breaker = CircuitBreaker(
    CB_FAILURE_THRESHOLD,
    CB_RECOVERY_TIMEOUT
)


# =========================================================
# HELPERS
# =========================================================

def increment_metric(metric_name):
    with metrics_lock:
        metrics[metric_name] += 1


def hash_pii(value: str) -> str:
    raw = f"{PII_SALT}{value}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def exponential_backoff(attempt: int):
    wait_time = BASE_BACKOFF * (2 ** attempt)
    jitter = random.uniform(0, 1)
    time.sleep(wait_time + jitter)


# =========================================================
# FETCH DOMAIN
# =========================================================

def fetch_vendor_domain(vendor: dict):

    if not circuit_breaker.can_execute():

        logging.warning(
            f"[CIRCUIT OPEN] Skipping vendor={vendor['vendor_id']}"
        )

        increment_metric("failed")
        return None

    for attempt in range(MAX_RETRIES):

        try:
            response = session.get(
                vendor["search_api_endpoint"],
                timeout=REQUEST_TIMEOUT,
                params={
                    "company": vendor["legal_name"],
                    "country": vendor["country"]
                }
            )

            if response.status_code == 200:

                circuit_breaker.record_success()
                increment_metric("success")

                return response.json().get("domain")

            elif response.status_code == 429:

                logging.warning(f"[429] Rate limit | vendor={vendor['vendor_id']} | attempt={attempt + 1}")

                increment_metric("retries")

                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    time.sleep(int(retry_after))
                else:
                    exponential_backoff(attempt)

            elif response.status_code == 503:

                logging.warning(f"[503] Service unavailable | vendor={vendor['vendor_id']} | attempt={attempt + 1}")

                increment_metric("retries")
                circuit_breaker.record_failure()

                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    time.sleep(int(retry_after))
                else:
                    exponential_backoff(attempt)

            else:

                circuit_breaker.record_failure()
                increment_metric("failed")
                return None

        except requests.RequestException:

            circuit_breaker.record_failure()
            increment_metric("network_errors")
            increment_metric("retries")

            exponential_backoff(attempt)

    increment_metric("failed")
    return None


# =========================================================
# PROCESS VENDOR
# =========================================================

def process_vendor(vendor: dict):

    domain = fetch_vendor_domain(vendor)

    return {
        "vendor_id": vendor["vendor_id"],
        "legal_name": vendor["legal_name"],
        "country": vendor["country"],
        "official_domain": domain,
        "tax_id_sha256": hash_pii(vendor["tax_id_pii"]),
        "processed_at": datetime.utcnow().isoformat(),
        "enrichment_status": "SUCCESS" if domain else "FAILED",
        "execution_date": execution_date
    }


# =========================================================
# MAIN
# =========================================================

def main():

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        vendors = json.load(f)

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(process_vendor, v) for v in vendors]

        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    results.append(r)
            except Exception:
                increment_metric("failed")

    # =====================================================
    # IDMPOTENTE WRITE (POR DÍA)
    # =====================================================

    json_path = os.path.join(OUTPUT_DIR, OUTPUT_JSON)
    parquet_path = os.path.join(OUTPUT_DIR, OUTPUT_PARQUET)

    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    df = pd.DataFrame(results)
    df.to_parquet(parquet_path, index=False)

    logging.info({
        "output_dir": OUTPUT_DIR,
        "success": metrics["success"],
        "failed": metrics["failed"],
        "retries": metrics["retries"],
        "network_errors": metrics["network_errors"]
    })


if __name__ == "__main__":
    main()
