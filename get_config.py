
from flask import Flask, jsonify, request
import requests

app = Flask(__name__)

# URL de las API de Strapi
STRAPI_DATALOGGERS_API = "http://0.0.0.0:1337/api/dataloggers"
STRAPI_SENSORES_API = "http://0.0.0.0:1337/api/sensores"

token = 'fd3c017eb295a0959b989d39b2613856c032a9b117ebb9432318133d7eab5d542dd40d5d84e6e18cd62b051e189ee28d2b89a686a9e3832bb5be9c53dbe395b04255d63d9c56a5e06f1e8493f27a240ec17003e864ec93465395b82dc141097d3d330c3a4e68182aa90a20c2a6191c4c513614cd16b4920d81eb0791a22a22d6'

def fetch_data(url, params):
    """Función auxiliar para realizar la solicitud GET a la API."""
    try:
        app.logger.info(f"Haciendo solicitud GET a {url} con los parámetros: {params}")
         # Configurar los encabezados con el token Bearer
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"  # Asegúrate de que el tipo de contenido sea JSON si es necesario
        }

        response = requests.get(url, params=params, headers=headers)
        app.logger.info(f"Respuesta recibida. Código de estado: {response.status_code}")
        response.raise_for_status()  # Lanza un error para códigos de estado 4xx/5xx
        app.logger.debug(f"Contenido de la respuesta: {response.text}")
        return response.json()
    except requests.RequestException as e:
        app.logger.error(f"Error al realizar la solicitud: {e}", exc_info=True)
        return -1  # Devolver -1 en caso de error

@app.route('/sensores', methods=['GET'])
def sensores():
    uid_machine = request.args.get('uidmachine')
    if not uid_machine:
        app.logger.warning("Parámetro 'uidmachine' no proporcionado en la solicitud.")
        return jsonify({"error": "UIDMachine parameter is required"}), 400

    
    params = {
        "filters[UidMachine][$eq]": uid_machine,
        "fields[0]": "MachineName",
        "fields[1]": "UidMachine",
        "populate[faena]": "*",
        "populate[faena_ubicacion]": "*",
        "populate[sensor_estado][populate]": "servidor_mqtt"
    }



    data = fetch_data(STRAPI_SENSORES_API, params)
    #print(data)

    if data == -1:
        return jsonify({"error": "Failed to fetch data from Strapi"}), 500

    app.logger.info(f"Datos obtenidos de Strapi: {data}")

    if 'data' in data and data['data']:
        sensor_data = data['data'][0]
        attributes = sensor_data.get("attributes", {})
        
        machine_name = attributes.get("MachineName", "N/A")
        faena = attributes.get("faena", {}).get("data", {}).get("attributes", {}).get("Faena", "N/A")
        faena_ubicacion = attributes.get("faena_ubicacion", {}).get("data", {}).get("attributes", {}).get("FaenaUbicacion", "N/A")
        #servidor_mqtt = attributes.get("sensor_estado", {}).get("data", {}).get("attributes", {}).get("servidor_mqtt", {}).get("data", {}).get("attributes", {}).get("mqtt_topic", "N/A")
        servidor_mqtt_data = attributes.get("sensor_estado", {}).get("data", {}).get("attributes", {}).get("servidor_mqtt", {}).get("data", {}).get("attributes", {})
        
        # Elimina lo que no usamos
        servidor_mqtt_data.pop("createdAt", None)
        servidor_mqtt_data.pop("updatedAt", None)
        servidor_mqtt_data.pop("publishedAt", None)
        
       
        topic = f"{faena}/sensores/{faena_ubicacion}/{machine_name}"
        app.logger.info(f"Tópico generado: {topic}")
        return jsonify({"topic": topic, "mqtt_topic": servidor_mqtt_data}), 200

    app.logger.warning(f"No se encontraron datos para el uidmachine: {uid_machine}")
    return jsonify({"error": "No data found for the specified UIDMachine"}), 404


@app.route('/datalogger', methods=['GET'])
def datalogger():
    machine_id = request.args.get('machineid')
    if not machine_id:
        app.logger.warning("Parámetro 'machineid' no proporcionado en la solicitud.")
        return jsonify({"error": "Machineid parameter is required"}), 400

    params = {
        "filters[MachineId][$eq]": machine_id,
        "fields[0]": "MachineName",
        "fields[1]": "id",
        "populate[faena]": "*",
        "populate[faena_ubicacion]": "*",
        "populate[datalogger_estado][populate]": "servidor_mqtt"
    }

    data = fetch_data(STRAPI_DATALOGGERS_API, params)
    
    if data == -1:
        return jsonify({"error": "Failed to fetch data from Strapi"}), 500

    app.logger.info(f"Datos obtenidos de Strapi: {data}")

    if 'data' in data and data['data']:
        datalogger_data = data['data'][0]
        attributes = datalogger_data.get('attributes', {})
        
        machine_name = attributes.get('MachineName', 'N/A')
        faena = attributes.get('faena', {}).get('data', {}).get('attributes', {}).get('Faena', 'N/A')
        faena_ubicacion = attributes.get('faena_ubicacion', {}).get('data', {}).get('attributes', {}).get('FaenaUbicacion', 'N/A')
        servidor_mqtt_data = attributes.get("datalogger_estado", {}).get("data", {}).get("attributes", {}).get("servidor_mqtt", {}).get("data", {}).get("attributes", {})
        
        # Elimina lo que no usamos
        servidor_mqtt_data.pop("createdAt", None)
        servidor_mqtt_data.pop("updatedAt", None)
        servidor_mqtt_data.pop("publishedAt", None)

        topic = f"{faena}/dataloggers/{faena_ubicacion}/{machine_name}"
        app.logger.info(f"Tópico generado: {topic}")
        return jsonify({"topic": topic, "mqtt_topic": servidor_mqtt_data}), 200

    app.logger.warning(f"No se encontraron datos para el machineid: {machine_id}")
    return jsonify({"error": "No data found for the specified machine name"}), 404

# Iniciar el servidor Flask
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
