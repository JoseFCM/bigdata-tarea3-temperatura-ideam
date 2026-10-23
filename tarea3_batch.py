"""
Tarea 3 - Big Data (UNAD) - Procesamiento en batch con Apache Spark
Autor: José Farith Caviedes Medina

Problema: análisis del comportamiento de la temperatura del aire en Colombia
durante 2025, a partir de la red de estaciones automáticas del IDEAM.

Dataset: "Temperatura Ambiente del Aire" (IDEAM), datos.gov.co, ID sbwg-7ju4.
Se trabaja con las observaciones de 2025 (~7 millones de registros),
almacenadas previamente en HDFS.

Ejecución:
    spark-submit --master spark://bigdata:7077 tarea3_batch.py
"""

import os
import time

import matplotlib
matplotlib.use("Agg")  # genera las gráficas como archivos PNG (sin entorno gráfico)
import matplotlib.pyplot as plt
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

RUTA_HDFS = "hdfs://localhost:9000/Tarea3/temperatura_2025.csv"
SALIDA_HDFS = "hdfs://localhost:9000/Tarea3/resultados"
DIR_GRAFICAS = os.path.expanduser("~/tarea3/graficas")
TEMP_MIN, TEMP_MAX = -10.0, 45.0  # rango físicamente plausible para Colombia (°C)
MARGEN = 5.0                      # °C de tolerancia fuera del rango normal (percentiles 1-99) de cada estación

spark = (SparkSession.builder.appName("Tarea3_Temperatura_IDEAM")
         .config("spark.executor.memory", "2g")
         .config("spark.sql.shuffle.partitions", "16")  # pocos núcleos: menos particiones, menos sobrecarga
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
os.makedirs(DIR_GRAFICAS, exist_ok=True)


def titulo(texto):
    print(f"\n{'=' * 70}\n{texto}\n{'=' * 70}")


# ---------------------------------------------------------------------------
# 1. CARGA desde HDFS con esquema explícito (más rápido que inferSchema en 7M de filas)
# ---------------------------------------------------------------------------
esquema = T.StructType([
    T.StructField("codigoestacion", T.StringType()),
    T.StructField("codigosensor", T.StringType()),
    T.StructField("fechaobservacion", T.StringType()),
    T.StructField("valorobservado", T.DoubleType()),
    T.StructField("nombreestacion", T.StringType()),
    T.StructField("departamento", T.StringType()),
    T.StructField("municipio", T.StringType()),
    T.StructField("zonahidrografica", T.StringType()),
    T.StructField("latitud", T.DoubleType()),
    T.StructField("longitud", T.DoubleType()),
    T.StructField("descripcionsensor", T.StringType()),
    T.StructField("unidadmedida", T.StringType()),
])
df_csv = spark.read.option("header", "true").schema(esquema).csv(RUTA_HDFS)

titulo("1. Esquema y muestra de los datos originales")
df_csv.printSchema()
df_csv.select("fechaobservacion", "nombreestacion", "departamento", "municipio", "valorobservado") \
      .show(5, truncate=False)

# Solo se conservan las columnas necesarias y se mantienen en memoria/disco para reutilizarlas
df = (df_csv.select("codigoestacion", "fechaobservacion", "valorobservado", "nombreestacion",
                    "departamento", "municipio", "zonahidrografica")
      .persist(StorageLevel.MEMORY_AND_DISK))
total = df.count()
print(f"Total de registros cargados desde HDFS: {total:,}")

# ---------------------------------------------------------------------------
# 2. LIMPIEZA: nulos, duplicados y valores atípicos
# ---------------------------------------------------------------------------
titulo("2. Limpieza y calidad de los datos")
df_sin_nulos = df.dropna(subset=["valorobservado", "fechaobservacion", "departamento"])
df_sin_dup = df_sin_nulos.dropDuplicates(["codigoestacion", "fechaobservacion"])

# Atípicos: fuera del rango físico, o muy lejos del rango normal de su propia estación.
# Se usan percentiles (1 % y 99 %) porque, a diferencia de la media y la desviación estándar,
# no se dejan arrastrar por los propios errores (por ejemplo, lecturas de 0 °C en zonas cálidas).
rango_est = df_sin_dup.groupBy("codigoestacion").agg(
    F.percentile_approx("valorobservado", 0.01).alias("p01"),
    F.percentile_approx("valorobservado", 0.99).alias("p99"))
df_marcado = (df_sin_dup.join(F.broadcast(rango_est), "codigoestacion")
              .withColumn("atipico",
                          (~F.col("valorobservado").between(TEMP_MIN, TEMP_MAX))
                          | (F.col("valorobservado") < F.col("p01") - MARGEN)
                          | (F.col("valorobservado") > F.col("p99") + MARGEN)))

# Un solo recorrido calcula todos los indicadores de calidad
calidad = df_marcado.agg(F.count("*").alias("sin_dup"),
                         F.sum(F.col("atipico").cast("int")).alias("atipicos")).first()
nulos = total - df_sin_nulos.count()
duplicados = total - nulos - calidad["sin_dup"]

# ---------------------------------------------------------------------------
# 3. TRANSFORMACIÓN: hora local de Colombia (los datos vienen en UTC), mes, hora y nombres
# ---------------------------------------------------------------------------
df_limpio = (df_marcado.filter(~F.col("atipico"))
             .withColumn("fecha_utc", F.to_timestamp("fechaobservacion", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
             .withColumn("fecha", F.from_utc_timestamp("fecha_utc", "America/Bogota"))
             .withColumn("mes", F.month("fecha"))
             .withColumn("hora", F.hour("fecha"))
             .withColumn("departamento",
                         F.regexp_replace(F.upper(F.trim("departamento")),
                                          "ARCHIPIELAGO DE SAN ANDRES.*", "SAN ANDRÉS Y PROVIDENCIA"))
             .withColumnRenamed("valorobservado", "temperatura")
             .select("codigoestacion", "nombreestacion", "departamento", "municipio",
                     "zonahidrografica", "fecha", "mes", "hora", "temperatura")
             .persist(StorageLevel.MEMORY_AND_DISK))
validos = df_limpio.count()
df.unpersist()

print(f"Registros con valores nulos:                    {nulos:,}")
print(f"Registros duplicados (misma estación y fecha):  {duplicados:,}")
print(f"Registros atípicos (fuera del rango físico o del normal de su estación): {calidad['atipicos']:,}")
print(f"Registros válidos para el análisis:             {validos:,} ({validos / total:.1%} del total)")

# ---------------------------------------------------------------------------
# 4. ANÁLISIS EXPLORATORIO (EDA) con DataFrames
# ---------------------------------------------------------------------------
titulo("3. Estadísticas generales de la temperatura (°C)")
df_limpio.select("temperatura").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()
print(f"Estaciones distintas: {df_limpio.select('codigoestacion').distinct().count():,}  |  "
      f"Departamentos: {df_limpio.select('departamento').distinct().count()}")

titulo("4. Temperatura por departamento (ordenado por promedio)")
por_depto = (df_limpio.groupBy("departamento")
             .agg(F.round(F.avg("temperatura"), 2).alias("promedio"),
                  F.round(F.min("temperatura"), 1).alias("minima"),
                  F.round(F.max("temperatura"), 1).alias("maxima"),
                  F.countDistinct("codigoestacion").alias("estaciones"),
                  F.count("*").alias("registros"))
             .orderBy(F.desc("promedio"))
             .cache())
por_depto.show(40, truncate=False)

titulo("5. Temperatura promedio por zona hidrográfica (10 más cálidas)")
df_limpio.groupBy("zonahidrografica") \
    .agg(F.round(F.avg("temperatura"), 2).alias("promedio"), F.count("*").alias("registros")) \
    .orderBy(F.desc("promedio")).show(10, truncate=False)

titulo("6. Comportamiento mensual (2025)")
por_mes = (df_limpio.groupBy("mes")
           .agg(F.round(F.avg("temperatura"), 2).alias("promedio"), F.count("*").alias("registros"))
           .orderBy("mes").cache())
por_mes.show(12)

titulo("7. Ciclo diario: temperatura promedio por hora (hora de Colombia)")
por_hora = df_limpio.groupBy("hora").agg(F.round(F.avg("temperatura"), 2).alias("promedio")).orderBy("hora").cache()
por_hora.show(24)

titulo("8. Las 5 estaciones más cálidas y las 5 más frías (promedio anual)")
por_estacion = (df_limpio.groupBy("nombreestacion", "municipio", "departamento")
                .agg(F.round(F.avg("temperatura"), 2).alias("promedio"), F.count("*").alias("registros"))
                .filter(F.col("registros") >= 1000)  # solo estaciones con datos suficientes
                .cache())
por_estacion.orderBy(F.desc("promedio")).show(5, truncate=False)
por_estacion.orderBy("promedio").show(5, truncate=False)

# ---------------------------------------------------------------------------
# 5. MISMO CÁLCULO CON RDDs: map + reduceByKey (promedio por departamento)
# ---------------------------------------------------------------------------
titulo("9. Cálculo con RDD (map y reduceByKey): promedio por departamento")
rdd = df_limpio.select("departamento", "temperatura").rdd
sumas = (rdd.map(lambda fila: (fila["departamento"], (fila["temperatura"], 1)))   # transformación map
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))                 # transformación reduceByKey
promedios_rdd = sumas.mapValues(lambda s: round(s[0] / s[1], 2))
print(f"Particiones del RDD: {rdd.getNumPartitions()}")
for depto, prom in promedios_rdd.takeOrdered(5, key=lambda x: -x[1]):            # acción takeOrdered
    print(f"  {depto:<28} {prom:>6} °C")
print("(Coincide con el resultado obtenido con DataFrames en la sección 4.)")

# ---------------------------------------------------------------------------
# 6. GUARDAR RESULTADOS EN HDFS
# ---------------------------------------------------------------------------
for nombre, tabla in [("por_departamento", por_depto), ("por_mes", por_mes), ("por_hora", por_hora)]:
    tabla.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{SALIDA_HDFS}/{nombre}")
print(f"\nResultados guardados en HDFS: {SALIDA_HDFS}/(por_departamento, por_mes, por_hora)")

# ---------------------------------------------------------------------------
# 7. VISUALIZACIÓN: los resultados agregados son pequeños y se traen al driver con collect()
# ---------------------------------------------------------------------------
deptos = sorted(por_depto.collect(), key=lambda r: r["promedio"])
mes = por_mes.collect()
hora = por_hora.collect()
hist = (df_limpio.withColumn("rango", F.floor("temperatura"))   # histograma calculado de forma distribuida
        .groupBy("rango").count().orderBy("rango").collect())
nombres_mes = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

plt.figure(figsize=(10, 9))
plt.barh([r["departamento"] for r in deptos], [r["promedio"] for r in deptos], color="#d9534f")
plt.xlabel("Temperatura promedio (°C)")
plt.title("Temperatura promedio del aire por departamento - IDEAM 2025")
plt.tight_layout()
plt.savefig(f"{DIR_GRAFICAS}/1_promedio_por_departamento.png", dpi=110)

plt.figure(figsize=(9, 4.5))
plt.plot([nombres_mes[r["mes"] - 1] for r in mes], [r["promedio"] for r in mes], marker="o", color="#f0ad4e")
plt.ylabel("Temperatura promedio (°C)")
plt.title("Temperatura promedio mensual en Colombia - IDEAM 2025")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(f"{DIR_GRAFICAS}/2_promedio_mensual.png", dpi=110)

plt.figure(figsize=(9, 4.5))
plt.plot([r["hora"] for r in hora], [r["promedio"] for r in hora], marker="o", color="#0275d8")
plt.xticks(range(0, 24))
plt.xlabel("Hora del día (hora de Colombia)")
plt.ylabel("Temperatura promedio (°C)")
plt.title("Ciclo diario de la temperatura del aire - IDEAM 2025")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(f"{DIR_GRAFICAS}/3_ciclo_diario.png", dpi=110)

plt.figure(figsize=(9, 4.5))
plt.bar([r["rango"] for r in hist], [r["count"] / 1000 for r in hist], width=0.9, color="#5cb85c")
plt.xlabel("Temperatura (°C)")
plt.ylabel("Miles de registros")
plt.title("Distribución de las temperaturas registradas - IDEAM 2025")
plt.tight_layout()
plt.savefig(f"{DIR_GRAFICAS}/4_distribucion.png", dpi=110)

print(f"Gráficas guardadas en: {DIR_GRAFICAS}")
print("\nLa interfaz web de Spark (http://localhost:4040) seguirá disponible 30 segundos...")
time.sleep(30)
spark.stop()
