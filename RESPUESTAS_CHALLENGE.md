# RESPUESTAS_CHALLENGE.md

# The OmniRisk Link Pipeline
## Respuestas Técnicas a las Consignas del Challenge

Autor: Raúl Díaz

---

# FASE 1 — Enriquecimiento de Entidades, APIs Inestables y Protección de PII

## 1. Automatización de búsqueda y resolución de entidades

El pipeline procesa el archivo `sap_vendors.json`, que contiene proveedores provenientes de SAP sin información de dominio web oficial.

Para cada proveedor se realiza una consulta HTTP utilizando:

- Nombre legal (`legal_name`)
- País (`country`)

La respuesta de la API es utilizada para obtener el dominio oficial de la empresa y enriquecer el dataset.

El procesamiento se ejecuta de forma concurrente mediante:

```python
ThreadPoolExecutor(max_workers=10)
```

permitiendo acelerar el enriquecimiento de múltiples proveedores simultáneamente.

---

## 2. Rate Limiting y resiliencia ante APIs inestables

La API simulada puede responder con errores:

- HTTP 429 (Too Many Requests)
- HTTP 503 (Service Unavailable)

Para garantizar resiliencia se implementaron los siguientes mecanismos:

### Exponential Backoff con Jitter

Cuando se recibe un error temporal, el sistema espera un tiempo creciente antes de reintentar:

```text
1s → 2s → 4s → 8s → ...
```

Además se agrega una componente aleatoria (jitter) para evitar tormentas de reintentos simultáneos.

### Lectura de Headers HTTP

Cuando la API devuelve el header:

```http
Retry-After
```

el pipeline respeta el tiempo indicado por el servicio antes de realizar una nueva solicitud.

### Circuit Breaker

Se implementó un patrón Circuit Breaker con tres estados:

- CLOSED
- OPEN
- HALF_OPEN

Cuando se supera el umbral configurado de fallos consecutivos:

```python
CB_FAILURE_THRESHOLD = 5
```

el circuito se abre y bloquea nuevas solicitudes temporalmente.

Luego de:

```python
CB_RECOVERY_TIMEOUT = 15 segundos
```

se permite una solicitud de prueba para validar la recuperación del servicio.

---

## 3. Protección de datos sensibles (PII)

El campo:

```text
tax_id_pii
```

contiene información tributaria sensible.

Antes de persistir los resultados se aplica:

- SHA-256
- Salt configurable mediante variable de entorno

Ejemplo conceptual:

```python
sha256(SALT + tax_id)
```

De esta forma los identificadores originales no son almacenados en los datasets procesados, reduciendo riesgos de exposición de datos personales.

---

# FASE 2 — Big Data, Spark y Optimización

## 1. Broadcast Join

El dataset:

```text
free_accounts.csv.gz
```

contiene cuentas que deben excluirse del análisis.

Dado que este dataset es pequeño, se utiliza Broadcast Join:

```python
broadcast(free_accounts_filtered)
```

Beneficios:

- Evita movimientos innecesarios de datos entre nodos.
- Reduce shuffles de red.
- Mejora significativamente el rendimiento.

El filtrado se realiza mediante:

```python
left_anti join
```

tanto para cuentas origen como destino.

---

## 2. Compactación de archivos (Small Files)

El pipeline escribe los datos finales en formato Parquet particionados por fecha:

```python
.partitionBy("transaction_day")
```

Previo a la escritura se realiza:

```python
.repartition("transaction_day")
```

con el objetivo de distribuir los datos según la clave de particionado.

Esta estrategia mejora la organización física de los datos y evita la generación excesiva de archivos pequeños.

En entornos productivos donde se requiera un único archivo por partición podría complementarse con:

```python
coalesce(...)
```

sin embargo, para este challenge se priorizó mantener paralelismo y estabilidad durante la escritura.

---

## 3. Afinamiento teórico de un clúster YARN

### Requerimiento

Utilizar aproximadamente el 50% de un clúster compuesto por:

- 3 nodos
- 50 GB RAM por nodo
- 12 CPU Cores por nodo

Capacidad total:

| Recurso | Total |
|----------|----------|
| RAM | 150 GB |
| CPU | 36 cores |

Objetivo (50%):

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

CPU:

```text
6 ejecutores × 3 cores = 18 cores
```

RAM:

```text
6 ejecutores × 10 GB = 60 GB
```

Overhead aproximado:

```text
6 × 1 GB = 6 GB
```

Driver:

```text
4 GB
```

Total:

```text
70 GB aprox.
```

Manteniéndose cerca del objetivo de utilizar la mitad del clúster.

### Capacity Scheduler

Se recomienda:

```xml
yarn.scheduler.capacity.root.analytics.capacity=50
```

reservando únicamente el 50% de la capacidad para este workload.

### Preemption

Se recomienda habilitar:

```xml
yarn.resourcemanager.scheduler.monitor.enable=true
```

permitiendo recuperar recursos cuando existan colas de mayor prioridad.

---

# FASE 3 — Orquestación Incremental con Apache Airflow

## Arquitectura del DAG

El DAG implementado posee tres etapas secuenciales:

```text
enrich_vendors
        ↓
process_transactions
        ↓
graph_export
```

Cada ejecución procesa una ventana temporal controlada utilizando la fecha lógica entregada por Airflow:

```text
{{ ds }}
```

---

## Idempotencia

La solución implementada garantiza re-ejecuciones limpias mediante:

### Particionado por fecha

Los datasets procesados se almacenan utilizando:

```text
day=YYYY-MM-DD
```

como clave de particionado.

### Escritura overwrite

Spark utiliza:

```python
.mode("overwrite")
```

junto con:

```python
spark.sql.sources.partitionOverwriteMode=dynamic
```

permitiendo reemplazar únicamente las particiones correspondientes al período procesado.

### Evolución a MERGE/Upsert

En un entorno productivo basado en tecnologías como:

- Delta Lake
- Apache Iceberg
- Apache Hudi

la estrategia recomendada sería implementar operaciones:

```sql
MERGE
```

o

```sql
UPSERT
```

para actualizar únicamente registros modificados manteniendo históricos completos.

---

## Conexión Multicloud Segura mediante OIDC

Para evitar el uso de credenciales estáticas se recomienda utilizar identidades federadas.

### AWS

Utilizar:

```text
IAM Roles for Service Accounts (IRSA)
```

permitiendo que Airflow obtenga credenciales temporales sin almacenar llaves.

### Google Cloud

Utilizar:

```text
Workload Identity Federation
```

para intercambiar tokens OIDC por credenciales temporales.

### Beneficios

- Eliminación de llaves permanentes.
- Menor superficie de ataque.
- Rotación automática de credenciales.
- Cumplimiento de buenas prácticas de seguridad.

---

# FASE 4 — Modelado y Análisis Temporal en Grafos

## Exportación a Neo4j

Las relaciones procesadas por Spark son exportadas a Neo4j mediante:

```cypher
MERGE (a:Account)
MERGE (b:Account)
MERGE (a)-[:GUARANTEE]->(b)
```

almacenando:

- Cuenta origen
- Cuenta destino
- Monto
- Fecha

---

## Detección de ciclos de riesgo

La implementación actual identifica ciclos de entre 2 y 3 niveles:

```cypher
MATCH p=(a:Account)-[:GUARANTEE*2..3]->(a)
RETURN count(p) AS cycle_count
```

Esta consulta permite detectar esquemas de garantías circulares potencialmente fraudulentos.

---

## Consulta Cypher para analizar la evolución temporal de los ciclos

Para visualizar la variación de montos a lo largo de la semana se propone:

```cypher
MATCH p=(a:Account)-[r:GUARANTEE*2..3]->(a)

UNWIND r AS rel

RETURN
    a.id AS account,
    rel.day AS day,
    sum(rel.amount) AS total_amount

ORDER BY day
```

Esta consulta permite observar la evolución temporal de los montos asociados a ciclos de garantías y detectar incrementos anómalos, incluyendo la mutación introducida a partir del Día 5 en los datos generados para el challenge.

---

# Conclusión

La solución desarrollada implementa:

- Enriquecimiento concurrente de proveedores.
- Manejo resiliente de APIs mediante Exponential Backoff y Circuit Breaker.
- Protección de datos PII utilizando SHA-256 con Salt.
- Procesamiento distribuido en Spark utilizando Broadcast Joins.
- Orquestación incremental mediante Apache Airflow.
- Exportación y análisis de relaciones de riesgo en Neo4j.
- Estrategias de idempotencia y diseño de arquitectura preparada para entornos productivos multicloud.

El pipeline fue desarrollado para ejecutarse completamente en entornos locales mediante Docker utilizando herramientas Open Source.