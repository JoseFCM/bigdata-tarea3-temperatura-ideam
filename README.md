# Análisis de la temperatura del aire en Colombia con Apache Spark (IDEAM 2025)

Tarea 3 – Procesamiento de Datos con Apache Spark · Curso Big Data (202016911) · Universidad Nacional Abierta y a Distancia (UNAD)
Autor: **José Farith Caviedes Medina**

## Problema
La red de estaciones automáticas del IDEAM registra la temperatura del aire varias veces por hora en todo el país, lo que genera millones de observaciones al año. Una base de datos convencional no es práctica para analizar ese volumen. Este proyecto usa **Apache Spark** para procesar los datos de 2025 de forma distribuida y responder:

- ¿Cuál es la temperatura promedio, mínima y máxima por departamento y por zona hidrográfica?
- ¿Cómo varía la temperatura a lo largo del año y a lo largo del día (ciclo diario)?
- ¿Cuáles son las estaciones más cálidas y más frías del país?
- ¿Qué tan confiables son los datos (nulos, duplicados y valores fuera de rango)?

## Conjunto de datos
- **Fuente:** [Temperatura Ambiente del Aire – IDEAM](https://www.datos.gov.co/d/sbwg-7ju4), portal de Datos Abiertos de Colombia (ID `sbwg-7ju4`).
- **Alcance:** observaciones del 1 de enero al 31 de diciembre de 2025 (~7 millones de registros, ~1,3 GB en CSV).
- **Columnas principales:** `codigoestacion`, `fechaobservacion`, `valorobservado` (°C), `nombreestacion`, `departamento`, `municipio`, `zonahidrografica`, `latitud`, `longitud`.

## Entorno
| Componente | Versión |
|---|---|
| Sistema operativo | Ubuntu Server 22.04.5 LTS (máquina virtual en VirtualBox) |
| Java | OpenJDK 11 |
| Hadoop (HDFS) | 3.3.6, modo pseudodistribuido |
| Apache Spark / PySpark | 3.5.8 (maestro standalone `spark://bigdata:7077`) |
| Apache Kafka | 3.9.2 (con ZooKeeper) |
| Python | 3.10 con `pyspark`, `matplotlib` y `kafka-python` |

## Estructura del repositorio
```
├── tarea3_batch.py                     # Procesamiento en batch (limpieza, transformación, EDA y gráficas)
├── streaming/
│   ├── kafka_producer.py               # Generador de lecturas simuladas de estaciones → topic sensor_data
│   └── spark_streaming_consumer.py     # Spark Structured Streaming: estadísticas por ventana de 1 minuto
└── graficas/                           # Gráficas generadas por tarea3_batch.py
```

## Ejecución del procesamiento en batch
1. Iniciar HDFS/YARN (usuario `hadoop`) y el clúster Spark:
   ```bash
   start-all.sh
   start-master.sh
   start-worker.sh spark://bigdata:7077
   ```
2. Descargar el dataset de 2025 y cargarlo en HDFS:
   ```bash
   curl -o temperatura_2025.csv "https://www.datos.gov.co/resource/sbwg-7ju4.csv?\$where=fechaobservacion%20between%20%272025-01-01T00:00:00%27%20and%20%272025-12-31T23:59:59%27&\$limit=8000000"
   hdfs dfs -mkdir -p /Tarea3
   hdfs dfs -put temperatura_2025.csv /Tarea3/
   ```
3. Ejecutar el análisis:
   ```bash
   spark-submit --master spark://bigdata:7077 tarea3_batch.py
   ```
   El programa muestra los resultados en consola, guarda las tablas agregadas en HDFS (`/Tarea3/resultados`) y genera las gráficas PNG en `~/tarea3/graficas`.

### Qué hace `tarea3_batch.py`
1. **Carga** el CSV desde HDFS con un esquema explícito.
2. **Limpieza:** elimina nulos, duplicados (misma estación y fecha) y temperaturas fuera del rango −10 °C a 50 °C.
3. **Transformación:** convierte la fecha a timestamp y deriva mes y hora.
4. **Análisis exploratorio (DataFrames):** estadísticas generales; agregaciones por departamento, zona hidrográfica, mes y hora; estaciones más cálidas y más frías.
5. **RDD:** calcula el promedio por departamento con `map` y `reduceByKey`.
6. **Visualización:** 4 gráficas (promedio por departamento, comportamiento mensual, ciclo diario y distribución).

## Procesamiento en tiempo real (Kafka + Spark Streaming)
```bash
# Terminal 1: ZooKeeper y Kafka
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data

# Terminal 1: productor
python3 streaming/kafka_producer.py

# Terminal 2: consumidor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 streaming/spark_streaming_consumer.py
```
El productor simula lecturas de 10 estaciones en distintos departamentos (una por segundo). El consumidor calcula, por ventana de 1 minuto y estación, el número de eventos, la temperatura promedio y máxima y la humedad promedio.

## Fuente de datos
Instituto de Hidrología, Meteorología y Estudios Ambientales [IDEAM]. (s. f.). *Temperatura Ambiente del Aire* [Conjunto de datos]. Datos Abiertos Colombia. https://www.datos.gov.co/d/sbwg-7ju4
