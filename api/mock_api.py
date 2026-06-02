from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
import random
import time
import os
from collections import defaultdict

app = FastAPI(title="OmniRisk Mock Search API")


# =========================================================
# MODE CONTROL (CLAVE PARA DEMO)
# =========================================================

# DEMO_MODE = stable (0) | stress (1)
DEMO_MODE = int(os.getenv("OMNIRISK_DEMO_MODE", "1"))


# =========================================================
# CONFIGURACION
# =========================================================

if DEMO_MODE == 1:
    # modo challenge (inestable)
    RATE_LIMIT_PROBABILITY = 0.15
    SERVICE_DOWN_PROBABILITY = 0.10
    MIN_DELAY = 0.1
    MAX_DELAY = 1.2
else:
    # modo demo estable (IMPORTANTE PARA PRESENTACION)
    RATE_LIMIT_PROBABILITY = 0.02
    SERVICE_DOWN_PROBABILITY = 0.01
    MIN_DELAY = 0.05
    MAX_DELAY = 0.2


# Rate limiting simple por endpoint
request_counter = defaultdict(int)


# =========================================================
# DOMINIOS FALSOS
# =========================================================

FAKE_DOMAINS = {
    "novatech": "novatech.cl",
    "andina": "andinaseguros.pe",
    "patagonia": "patagoniaguarantees.com.br",
    "mexicredit": "mexicredit.mx",
    "apex": "apexfinance.cl",
    "pacifico": "pacificoavales.pe",
    "boreal": "borealinsurtech.com.br",
    "atlantica": "atlanticagarantias.mx",
    "sierra": "sierraventure.cl",
    "austral": "australleasing.pe",
}


# =========================================================
# RESOLVER DOMINIO
# =========================================================

def resolve_domain(company_name: str) -> str:
    lower_name = company_name.lower()

    for keyword, domain in FAKE_DOMAINS.items():
        if keyword in lower_name:
            return domain

    return "unknown-company.com"


# =========================================================
# ENDPOINT PRINCIPAL
# =========================================================

@app.get("/mock_search_api/{vendor_id}")
def mock_search_api(
    vendor_id: int,
    response: Response,
    company: str = "",
    country: str = ""
):

    # =====================================================
    # LATENCIA SIMULADA
    # =====================================================

    simulated_delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(simulated_delay)

    request_counter[vendor_id] += 1

    # =====================================================
    # ERROR 429 (RATE LIMIT)
    # =====================================================

    if random.random() < RATE_LIMIT_PROBABILITY:

        retry_after = random.randint(1, 3)

        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "message": "Rate limit exceeded"
            },
            headers={
                "Retry-After": str(retry_after),
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Remaining": str(random.randint(0, 2)),
            }
        )

    # =====================================================
    # ERROR 503 (SERVICE INSTABILITY)
    # =====================================================

    if random.random() < SERVICE_DOWN_PROBABILITY:

        retry_after = random.randint(2, 6)

        return JSONResponse(
            status_code=503,
            content={
                "error": "Service Unavailable",
                "message": "Temporary upstream failure"
            },
            headers={
                "Retry-After": str(retry_after)
            }
        )

    # =====================================================
    # RESPUESTA EXITOSA
    # =====================================================

    domain = resolve_domain(company)

    response.headers["X-RateLimit-Limit"] = "100"
    response.headers["X-RateLimit-Remaining"] = str(random.randint(10, 99))

    return {
        "vendor_id": vendor_id,
        "company_name": company,
        "country": country,
        "domain": domain,
        "confidence_score": round(random.uniform(0.80, 0.99), 2),
        "resolved_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }


# =========================================================
# HEALTHCHECK
# =========================================================

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "mode": "stress" if DEMO_MODE == 1 else "stable"
    }
