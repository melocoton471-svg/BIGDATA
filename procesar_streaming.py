import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    round as spark_round,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_SERVIDOR = os.getenv("KAFKA_SERVIDOR", "localhost:9092")
TOPICO = os.getenv("TOPICO", "datos_meteorologicos")
VENTANA = os.getenv("VENTANA", "1 minute")
MARCA_AGUA = os.getenv("MARCA_AGUA", "2 minutes")
CARPETA_CHECKPOINT = "checkpoint_streaming"

ESQUEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
])


def procesar_streaming():
    spark = (
        SparkSession.builder.appName("ProcesamientoStreamingMeteorologico")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVIDOR)
        .option("subscribe", TOPICO)
        .option("startingOffsets", "latest")
        .load()
    )

    datos = (
        crudo.select(from_json(col("value").cast("string"), ESQUEMA).alias("d"))
        .select("d.*")
        .dropna(subset=["timestamp", "temperature", "humidity", "wind_speed"])
        .withColumn("fecha_hora", to_timestamp(col("timestamp")))
    )

    resumen = (
        datos.withWatermark("fecha_hora", MARCA_AGUA)
        .groupBy(window(col("fecha_hora"), VENTANA))
        .agg(
            spark_round(avg("temperature"), 2).alias("temperatura_promedio_c"),
            spark_round(avg("humidity"), 2).alias("humedad_promedio_pct"),
            spark_round(avg("wind_speed"), 2).alias("viento_promedio_kmh"),
        )
        .select(
            col("window.start").alias("inicio"),
            col("window.end").alias("fin"),
            "temperatura_promedio_c",
            "humedad_promedio_pct",
            "viento_promedio_kmh",
        )
    )

    consulta = (
        resumen.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", CARPETA_CHECKPOINT)
        .start()
    )

    try:
        consulta.awaitTermination()
    except KeyboardInterrupt:
        print("\nDeteniendo el procesamiento en streaming...")
        consulta.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    procesar_streaming()
