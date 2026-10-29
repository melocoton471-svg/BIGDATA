import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

LATITUD = float(os.getenv("LATITUD", "3.5394"))       # Palmira, Colombia
LONGITUD = float(os.getenv("LONGITUD", "-76.3036"))
KAFKA_SERVIDOR = os.getenv("KAFKA_SERVIDOR", "localhost:9092")
TOPICO = os.getenv("TOPICO", "datos_meteorologicos")
INTERVALO_SEGUNDOS = int(os.getenv("INTERVALO_SEGUNDOS", "10"))

URL_API = "https://api.open-meteo.com/v1/forecast"


def consultar_clima():
    params = {
        "latitude": LATITUD,
        "longitude": LONGITUD,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "America/Bogota",
    }
    respuesta = requests.get(URL_API, params=params, timeout=30)
    respuesta.raise_for_status()
    actual = respuesta.json().get("current")
    if not actual:
        raise RuntimeError("La API no devolvió datos actuales. Verifica la conexión.")
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "temperature": actual["temperature_2m"],
        "humidity": actual["relative_humidity_2m"],
        "wind_speed": actual["wind_speed_10m"],
    }


def main():
    productor = KafkaProducer(
        bootstrap_servers=KAFKA_SERVIDOR,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Enviando datos al tópico '{TOPICO}' cada {INTERVALO_SEGUNDOS} s. Ctrl+C para detener.")
    try:
        while True:
            try:
                mensaje = consultar_clima()
                productor.send(TOPICO, mensaje)
                productor.flush()
                print(f"Enviado: {mensaje}")
            except requests.RequestException as error:
                print(f"Error al consultar la API: {error}")
            time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("\nProductor detenido.")
    finally:
        productor.close()


if __name__ == "__main__":
    main()
