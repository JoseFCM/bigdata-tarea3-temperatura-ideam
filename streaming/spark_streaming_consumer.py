"""
Consumidor con Spark Structured Streaming (basado en el Anexo 3 del curso).
Lee las lecturas del topic 'sensor_data' de Kafka y calcula, en ventanas de
1 minuto, la temperatura y humedad promedio, la temperatura máxima y el número
de eventos recibidos por estación.

Ejecución:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, max as max_, round as round_, window
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType, TimestampType

spark = SparkSession.builder.appName("KafkaSparkStreaming_IDEAM").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los mensajes JSON enviados por el productor
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("departamento", StringType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType()),
])

# Lector de streaming desde Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor_data")
      .load())

# Parsear el JSON del campo 'value'
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Estadísticas por ventana de 1 minuto y estación
windowed_stats = (parsed_df
                  .groupBy(window(col("timestamp"), "1 minute"), "sensor_id", "departamento")
                  .agg(count("*").alias("eventos"),
                       round_(avg("temperature"), 2).alias("temp_promedio"),
                       round_(max_("temperature"), 2).alias("temp_maxima"),
                       round_(avg("humidity"), 2).alias("humedad_promedio"))
                  .orderBy("window", "sensor_id"))

# Mostrar resultados en consola en cada micro-lote
query = (windowed_stats.writeStream
         .outputMode("complete")
         .format("console")
         .option("truncate", "false")
         .option("numRows", 30)
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()
