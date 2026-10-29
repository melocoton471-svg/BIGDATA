# BIGDATA
Procesamiento en streaming de datos meteorológicos con Kafka y Apache Spark
Descripción

Este proyecto consulta periódicamente la API de Open-Meteo, publica cada lectura en un tópico de Apache Kafka y la procesa en tiempo real con Spark Structured Streaming. Spark calcula promedios de temperatura, humedad relativa y velocidad del viento por ventanas de tiempo. La ubicación predeterminada es Palmira, Colombia.

A diferencia del procesamiento batch, que analiza un archivo ya completo, el streaming procesa los datos a medida que llegan.

Arquitectura
Open-Meteo API -> productor_kafka.py -> Kafka (tópico) -> procesar_streaming.py (Spark) -> consola
Tecnologías
Python 3
Apache Kafka
Apache Spark (PySpark, Structured Streaming)
API de Open-Meteo
Requisitos
Ubuntu o Linux
Python 3 y pip
Apache Kafka instalado y en ejecución (localhost:9092)
Apache Spark instalado y spark-submit disponible en la terminal
Java compatible con las versiones de Spark y Kafka
Conexión a internet para consultar Open-Meteo
Instalación
python3 -m pip install -r requirements.txt
spark-submit --version
Ejecución
Inicia Kafka y crea el tópico (desde la carpeta de Kafka):
bin/kafka-topics.sh --create --topic datos_meteorologicos --bootstrap-server localhost:9092
En una terminal, inicia el productor:
python3 productor_kafka.py
En otra terminal, inicia el procesamiento con Spark. El paquete de Kafka debe coincidir con tu versión de Spark y Scala (ejemplo para Spark 3.5.1 con Scala 2.12):
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 procesar_streaming.py

Spark mostrará en pantalla los promedios de cada ventana a medida que lleguen datos. Detén ambos procesos con Ctrl+C.

Configuración

Se controla con variables de entorno:

LATITUD=4.7110 
LONGITUD=-74.0721 
python3 productor_kafka.py
INTERVALO_SEGUNDOS=5 python3 productor_kafka.py
VENTANA="2 minutes" spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 procesar_streaming.py

Otras variables: KAFKA_SERVIDOR, TOPICO y MARCA_AGUA.

¿Qué hace el código?
productor_kafka.py consulta el clima actual en Open-Meteo cada pocos segundos.
Publica cada lectura como JSON en el tópico de Kafka.
procesar_streaming.py lee el tópico como un flujo continuo con Spark.
Convierte el JSON a columnas y descarta filas con valores faltantes.
Agrupa por ventanas de tiempo (con marca de agua para datos tardíos) y calcula los promedios.
Muestra los resultados en consola.
Estructura del repositorio
proyecto-bigdata-streaming/
productor_kafka.py
procesar_streaming.py
requirements.txt
gitignore
README.md
Publicar en GitHub
git init
git add README.md productor_kafka.py procesar_streaming.py requirements.txt .gitignore
git commit -m "Agregar procesamiento streaming meteorologico con Kafka y Spark"
git branch -M main
git remote add origin https://github.com/Andru Vidal/proyecto-bigdata-streaming.git
git push -u origin main

Reemplaza TU-USUARIO por tu nombre de usuario de GitHub.

Fuente de datos

Open-Meteo: https://open-meteo.com/ (API de pronóstico y clima actual: https://api.open-meteo.com/v1/forecast)
