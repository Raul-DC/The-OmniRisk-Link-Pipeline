# RESPUESTAS_CHALLENGE.md

# The OmniRisk Link Pipeline
## Respuestas a las Consignas del Challenge Técnico

---

# FASE 1 — Enriquecimiento de Entidades, APIs Inestables y Protección de PII

---

## Consigna 1

> Automatización de Búsqueda (El Caso Leo): Para cada proveedor, automatiza la consulta a la API combinando nombre legal + país para parsear y extraer el dominio web oficial.

### Respuesta

El pipeline procesa el archivo `sap_vendors.json`, que contiene proveedores provenientes de SAP sin dominio web oficial.

Para cada proveedor se realiza una consulta HTTP utilizando:

- `legal_name`
- `country`

La respuesta de la API es utilizada para enriquecer el dataset con el dominio oficial de la organización.

El procesamiento se ejecuta concurrentemente mediante:

```python
ThreadPoolExecutor(max_workers=10)
```

lo que permite procesar múltiples proveedores en paralelo y reducir significativamente el tiempo total de ejecución.

---

## Consigna 2

> Rate Limiting y Resiliencia de Red: La API retorna errores aleatorios 429 y 503. El script debe interpretar los headers HTTP, aplicar Exponential Backoff con ruido e implementar un Circuit Breaker que detenga peticiones si la API falla de forma persistente.

### Respuesta

Para manejar errores transitorios y evitar sobrecargar servicios externos se implementaron los siguientes mecanismos:

### Exponential Backoff con Jitter

Ante errores recuperables, el sistema incrementa progresivamente el tiempo de espera entre reintentos:

```text
1s → 2s → 4s → 8s → ...
```

Además se agrega una componente aleatoria (jitter) para evitar reintentos simultáneos.

### Lectura de Headers HTTP

Cuando la API responde con:

```http
Retry-After
```

el pipeline respeta el tiempo indicado antes de volver a intentar la solicitud.

### Circuit Breaker

Se implementó un patrón Circuit Breaker con los estados:

- CLOSED
- OPEN
- HALF_OPEN

Configuración utilizada:

```python
CB_FAILURE_THRESHOLD = 5
CB_RECOVERY_TIMEOUT = 15
```

Cuando se alcanza el umbral de errores consecutivos, el circuito se abre temporalmente y bloquea nuevas solicitudes. Luego de transcurrido el tiempo de recuperación, se habilita una solicitud de prueba para verificar si el servicio volvió a estar disponible.

---

## Consigna 3

> Enmascaramiento de Datos Sensibles (PII): Antes de persistir, enmascara el campo tax_id_pii aplicando un hasheo criptográfico seguro SHA-256 con Salt.

### Respuesta

Antes de persistir la información procesada se protege el campo:

```text
tax_id_pii
```

mediante un algoritmo SHA-256 combinado con un Salt configurable.

Implementación conceptual:

```python
sha256(SALT + tax_id)
```

Esta estrategia permite:

- Evitar almacenar identificadores tributarios en texto plano.
- Reducir riesgos de exposición de datos personales.
- Cumplir principios básicos de protección de información sensible.

---

# FASE 2 — Big Data, Spark y Optimización

---

## Consigna 1

> Broadcast Join: Realiza un cruce optimizado en memoria para excluir las cuentas de free_accounts.csv.gz, garantizando Cero Shuffles en red.

### Respuesta

El archivo:

```text
free_accounts.csv.gz
```

contiene cuentas promocionales que deben excluirse del análisis.

Dado que se trata de un dataset pequeño, se utiliza Spark Broadcast Join:

```python
broadcast(free_accounts_filtered)
```

Esta estrategia distribuye el dataset pequeño a todos los ejecutores y evita movimientos innecesarios de datos entre nodos.

La exclusión se realiza mediante:

```python
left_anti join
```

tanto para cuentas origen como destino.

Beneficios:

- Menor tráfico de red.
- Menor costo de shuffle.
- Mejor rendimiento general del pipeline.

---

## Consigna 2

> Compactación (Small Files): Coalesce los registros diarios Parquet para asegurar un archivo grande y saludable por partición.

### Respuesta

La solución implementada escribe los datos finales utilizando particionado por fecha:

```python
.partitionBy("transaction_day")
```

y previamente realiza:

```python
.repartition("transaction_day")
```

para distribuir correctamente los registros antes de la escritura.

Esta estrategia mejora la organización física de los datos y reduce la proliferación de archivos pequeños.

En escenarios productivos donde se requiera exactamente un archivo por partición podría utilizarse:

```python
coalesce(...)
```

Sin embargo, para este challenge se priorizó mantener paralelismo y estabilidad durante el proceso de escritura.

---

## Consigna 3

> Afinamiento del Cluster YARN: Diseña los parámetros Spark de submit para usar exactamente la mitad de un clúster físico de 3 nodos (50GB RAM y 12 Cores c/u), detallando ejecutores, memoria, overhead, preemption y colas de CapacityScheduler.

### Respuesta

### Capacidad total del clúster

| Recurso | Total |
|----------|----------|
| RAM | 150 GB |
| CPU | 36 cores |

### Objetivo (50%)

| Recurso | Objetivo |
|----------|----------|
| RAM | 75 GB |
| CPU | 18 cores |

### Configuración propuesta

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 6 \
  --executor-cores 3 \
  --executor-memory 10G \
  --driver-memory 4G
```

### Justificación

CPU utilizada:

```text
6 × 3 = 18 cores
```

RAM utilizada:

```text
6 × 10 GB = 60 GB
```

Overhead aproximado:

```text
6 GB
```

Driver:

```text
4 GB
```

Total estimado:

```text
70 GB
```

lo que se aproxima al objetivo de utilizar aproximadamente la mitad de los recursos disponibles.

### Capacity Scheduler

Configuración sugerida:

```xml
yarn.scheduler.capacity.root.analytics.capacity=50
```

### Preemption

Configuración sugerida:

```xml
yarn.resourcemanager.scheduler.monitor.enable=true
```

permitiendo reasignar recursos cuando existan colas con mayor prioridad.

---

# FASE 3 — Orquestación Incremental con Apache Airflow

---

## Consigna

> Construye un DAG en Apache Airflow que orqueste la ventana de 7 días de forma incremental. Debe garantizarse la idempotencia mediante lógicas de MERGE/Upsert y explicar la conexión multicloud segura mediante Identidades Federadas (OIDC) y accesos privados sin llaves estáticas de AWS/GCP.

### Respuesta — DAG Incremental

Se implementó un DAG compuesto por tres etapas:

```text
enrich_vendors
        ↓
process_transactions
        ↓
graph_export
```

Cada ejecución utiliza la fecha lógica proporcionada por Airflow:

```text
{{ ds }}
```

permitiendo procesar una ventana temporal específica de forma controlada.

---

### Respuesta — Idempotencia

La solución implementada utiliza:

- Particionado por fecha.
- Escritura overwrite controlada.
- Reprocesamiento seguro de ventanas temporales.

Configuración utilizada:

```python
.mode("overwrite")
```

junto con:

```python
spark.sql.sources.partitionOverwriteMode=dynamic
```

De esta manera, una reejecución reemplaza únicamente las particiones correspondientes al período procesado.

En un entorno productivo basado en tecnologías como:

- Delta Lake
- Apache Iceberg
- Apache Hudi

sería recomendable evolucionar hacia operaciones:

```sql
MERGE
```

o

```sql
UPSERT
```

para minimizar escrituras y preservar históricos completos.

---

### Respuesta — OIDC e Identidades Federadas

Para evitar credenciales estáticas se recomienda utilizar identidades federadas.

AWS:

```text
IAM Roles for Service Accounts (IRSA)
```

Google Cloud:

```text
Workload Identity Federation
```

Beneficios:

- Eliminación de llaves permanentes.
- Credenciales temporales.
- Rotación automática.
- Menor superficie de ataque.
- Cumplimiento de buenas prácticas de seguridad.

---

# FASE 4 — Modelado y Análisis Temporal en Grafos

---

## Consigna

> Carga los nodos y aristas con timestamps en Neo4j (o procesa en NetworkX). Escribe una query Cypher que identifique ciclos de retroalimentación de garantías de hasta 3 niveles de profundidad, mostrando de forma analítica la variación de montos del ciclo a lo largo de la semana (reflejando la mutación del Día 5).

### Respuesta — Carga de Grafos

Las relaciones procesadas por Spark son exportadas a Neo4j utilizando:

```cypher
MERGE (a:Account)
MERGE (b:Account)
MERGE (a)-[:GUARANTEE]->(b)
```

almacenando:

- Cuenta origen.
- Cuenta destino.
- Fecha.
- Monto.

---

### Respuesta — Detección de Ciclos

La implementación actual detecta ciclos mediante:

```cypher
MATCH p=(a:Account)-[:GUARANTEE*2..3]->(a)
RETURN count(p) AS cycle_count
```

lo que permite identificar estructuras circulares de garantías potencialmente riesgosas.

---

### Respuesta — Variación Temporal de Montos

Para analizar la evolución temporal de los ciclos a lo largo de la semana se propone la siguiente consulta:

```cypher
MATCH p=(a:Account)-[r:GUARANTEE*2..3]->(a)

UNWIND r AS rel

RETURN
    a.id AS account,
    rel.day AS day,
    sum(rel.amount) AS total_amount

ORDER BY day
```

Esta consulta permite observar cómo evolucionan los montos asociados a ciclos de garantías y facilita detectar incrementos anómalos, incluyendo la mutación introducida a partir del Día 5 en los datos del challenge.

---

# Conclusión

La solución implementada cubre los principales requerimientos planteados por el challenge:

- Enriquecimiento concurrente de proveedores.
- Resiliencia ante APIs inestables mediante Exponential Backoff y Circuit Breaker.
- Protección de datos sensibles utilizando SHA-256 con Salt.
- Procesamiento distribuido con Spark y Broadcast Join.
- Orquestación incremental mediante Apache Airflow.
- Modelado y análisis de relaciones de riesgo utilizando Neo4j.
- Estrategias de idempotencia y diseño preparado para entornos productivos multicloud.
