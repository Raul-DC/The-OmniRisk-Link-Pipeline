# The OmniRisk Link Pipeline

Pipeline de análisis de riesgo financiero desarrollado para el challenge técnico **The OmniRisk Link Pipeline**.

El proyecto procesa transacciones financieras, enriquece información de proveedores mediante APIs externas, genera métricas de riesgo utilizando Apache Spark y exporta relaciones de garantía a Neo4j para la detección de patrones sospechosos y ciclos de fraude.

---

# Arquitectura

```text
SAP Vendors
      │
      ▼
enrich_vendors.py
(Fase 1 - TPRM)
      │
      ▼
process_transactions.py
(Fase 2 - Spark)
      │
      ▼
graph_export.py
(Fase 4 - Neo4j)
```

La orquestación completa se realiza mediante Apache Airflow.

---

# Tecnologías utilizadas

- Python 3.11
- Apache Spark 3.5
- Apache Airflow 2.7
- Neo4j 5
- Docker Compose
- Pandas
- Network Analysis (Neo4j / Cypher)

---

# Estructura del proyecto

```text
omnIRisk-project/
│
├── airflow/
│   ├── dags/
│   │   └── omnirisk_pipeline.py/
│   └── logs/
│
├── api/
│   └── mock_api.py/
│
├── data/
│   ├── curated/
│   │   ├── graph_edges/
│   │   ├── high_risk_accounts/
│   │   ├── risk_metrics/
│   │   └── transactions/
│   ├── processed/
│   ├── raw/
│   ├── transactions/
│   ├── free_accounts.csv.gz
│   └── sap_vendors.json
│
├── etl/
│   └── enrich_vendors.py
│
├── neo4j/
│   └── graph_export.py
│
├── scripts/
│   ├── __pycache__/
│   ├── circuit_breaker.py
│   └── generate_challenge_data.py
│
├── spark/
│   └── process_transactions.py
│
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements-airflow.txt
├── requirements-api.txt
├── requirements-shared.txt
└── requirements-spark.txt
```

---

# Componentes principales

## 1. Enriquecimiento de proveedores

Archivo:

```text
etl/enrich_vendors.py
```

Funcionalidades:

- Lectura de proveedores desde SAP
- Llamadas concurrentes a API externa
- Manejo de errores 429 y 503
- Exponential Backoff
- Circuit Breaker
- Enmascaramiento de PII mediante SHA-256
- Escritura de resultados en JSON y Parquet

Salida:

```text
data/processed/enriched_vendors/
```

---

## 2. Procesamiento distribuido con Spark

Archivo:

```text
spark/process_transactions.py
```

Funcionalidades:

- Validación de calidad de datos
- Filtrado de transacciones aprobadas
- Broadcast Join con cuentas promocionales
- Cálculo de métricas de riesgo
- Identificación de cuentas de alto volumen
- Generación de capa de grafos

Salidas:

```text
data/curated/transactions/
data/curated/risk_metrics/
data/curated/high_risk_accounts/
data/curated/graph_edges/
```

---

## 3. Exportación a Neo4j

Archivo:

```text
neo4j/graph_export.py
```

Funcionalidades:

- Lectura de relaciones generadas por Spark
- Carga masiva en Neo4j
- Creación de nodos Account
- Creación de relaciones GUARANTEE
- Detección de ciclos de garantía

---

# Orquestación con Airflow

DAG:

```text
airflow/dags/omnirisk_pipeline.py
```

Flujo:

```text
enrich_vendors
        ↓
process_transactions
        ↓
graph_export
```

Frecuencia:

```text
@daily
```

Características:

- Retries automáticos
- Timeout por tarea
- Ejecución secuencial
- Una ejecución activa a la vez

---

# Cómo ejecutar el proyecto

## 1. Levantar contenedores

```bash
docker compose up -d
```

## 2. Verificar estado

```bash
docker compose ps
```

## 3. Acceder a las interfaces

### Airflow

```text
http://localhost:8080
```

### Neo4j Browser

```text
http://localhost:7474
```

### Spark / Jupyter

```text
http://localhost:8888
```

---

# Credenciales Neo4j

Usuario:

```text
neo4j
```

Contraseña:

```text
<password>
```

Puertos:

| Puerto | Uso |
|----------|----------|
| 7474 | Neo4j Browser |
| 7687 | Bolt Protocol |

---

# Ejemplo de consulta Cypher

Detectar ciclos de garantía de hasta tres niveles:

```cypher
MATCH p=(a:Account)-[:GUARANTEE*1..3]->(a)
RETURN p
```

---

# Datos generados

El pipeline produce:

- Proveedores enriquecidos
- Métricas de riesgo
- Cuentas de alto riesgo
- Relaciones para análisis de grafos
- Detección de ciclos financieros

---

# Autor

Raúl Díaz

Challenge técnico: **The OmniRisk Link Pipeline**
