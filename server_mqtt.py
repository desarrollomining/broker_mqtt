import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import logging
from datetime import datetime
# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de los tokens para cada organización
tokens = {
    "Antucoya": "fX0wkWg_8T1rIAInD7I0IRHsZFMPf28oYPRn6JLG_1rddFFyVO1NZvlQRDm7sXAy6-WkQztQUwPbN1Btd_7brQ==",
    "Ministro Hales": "C6F2ceIQZZ5KX8gktrD-YJKCgjGX9m8fruW2kXMibbOUS6Rd435FLqiSfb6-mv_e_0E4W9XYSaDhtqKeOaw35Q==",
    "Mining": "tF9jECwmbuLXCBxr2OhfzEWH2TWQtKe_Am-gZo56a5SEmtD8-7S-_-C4eVjVvdzvgYpax3e8ecsTlgetJ7hJ8g==",
    "Centinela": "MS2rViOTB1Qq7m6gqWpsQhwKPnUTmdufDoYjkA1W4xpuTsz8n7cGlpBKkp_4et1w1MwRzvoOeUqMEl7WPr8Zhw==",
    "Candelaria": "B6rlXjPtjsAIUm2aZhf9iZnYtpWy77cE-W5VvQJiE-tY7FSIiGOcvKYRBoVZ2lz3hR_qpgtf33dXvc5rYCOAwg==",
    "TallerStgo": "Hvom5mZxb4oSyEiMaeLtwlH6GrxUCGVCZ05mcK_lMyIh4geIQy7LTSupRWDvIYvT9MFnHWvzsRE_unpv9lKQaQ=="
}

# Configuración del servidor InfluxDB y buckets por organización
org_buckets = {
    "Antucoya": {
        "dataloggers": "dataloggers",
        "sensores": "sensores",
    },
    "Ministro Hales": {
        "dataloggers": "dataloggers",
        "sensores": "sensores",
    },
    "Centinela": {
        "dataloggers": "dataloggers",
        "sensores": "sensores",
    },
    "Candela": {
        "dataloggers": "dataloggers",
        "sensores": "sensores",
    },
    "Mining": {
        "equipos_terreno": "Infraestructura",
        "desarrollo": "desarrollo",
    },
    "TallerStgo": {
        "sensores": "sensores"
    }
}

# Configuración del cliente MQTT
broker = "desarrollo.mine-360.com"
port = 1883
username = "admin"
password = "Mining2015"

# Función para seleccionar el token y bucket según la organización y topic
def get_influxdb_config(topic):
    try:
        parts = topic.split("/")
        organization = parts[0]
        bucket = parts[1]
        ubicacion = parts[2]
        id_equipo = parts[3]

        token = tokens.get(organization)
        if not token:
            logging.error(f"Token no encontrado para la organización: {organization}")
            return None, None, None, None, None

        return token, bucket, organization, ubicacion, id_equipo
    except IndexError:
        logging.error(f"Topic mal formado: {topic}")
        return None, None, None, None, None
    except Exception as e:
        logging.error(f"Error al obtener la configuración de InfluxDB: {e}")
        return None, None, None, None, None

# Función para conectarse a InfluxDB y escribir datos
def write_to_influxdb(topic, payload):
    try:
        token, bucket, organization, ubicacion, id_equipo = get_influxdb_config(topic)
        if not token or not bucket:
            logging.error(f"Configuración de InfluxDB inválida para el topic: {topic}")
            return

        with InfluxDBClient(url="http://localhost:8086", token=token, org=organization) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            data = json.loads(payload)

            # Validar que el payload contiene 'measurement'
            if 'measurement' not in data:
                logging.error(f"Payload no contiene 'measurement': {payload}")
                return

                        
            time_value = data.get('time', None)
            #time_value_2 = datetime.utcfromtimestamp(data.get('time', 0)).strftime('%A, %d %B %Y %H:%M:%S')
            measurements = data['measurement']

            # Crear punto para InfluxDB
            point = Point(ubicacion)
            point.tag("equipo", id_equipo)

            for key, value in measurements.items():
                if isinstance(value, str):
                    point.field(key, value)
                else:
                    point.field(key, float(value))

            if time_value:
                point.time(int(time_value), WritePrecision.MS)

            write_api.write(bucket=bucket, org=organization, record=point)
            logging.info(f"Datos escritos en InfluxDB: {measurements}, time = {time_value}")
            logging.info(f"bucket: {bucket}, organization: {organization}, record: {point}")
            #logging.info(f"time_value: {time_value}, time_value_2: {time_value_2}")
            logging.info(f"***************************************************************")

    except json.JSONDecodeError:
        logging.error(f"Error al decodificar el payload JSON: {payload}")
    except Exception as e:
        logging.error(f"Error al escribir en InfluxDB: {e}")

# Función de callback cuando se recibe un mensaje MQTT
def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode()
        logging.info(f"Mensaje recibido en {topic}: {payload}")

        write_to_influxdb(topic, payload)
    except Exception as e:
        logging.error(f"Error en el procesamiento del mensaje MQTT: {e}")

# Función de conexión MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Conexión exitosa al broker MQTT")
        client.subscribe("#")
    else:
        logging.error(f"Error al conectar, código de error: {rc}")

# Inicializar cliente MQTT
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(username, password)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Conectar al broker MQTT
mqtt_client.connect(broker, port, 60)

# Bucle de espera de mensajes
mqtt_client.loop_forever()
