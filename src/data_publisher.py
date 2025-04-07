#!/usr/bin/env python3
"""
Servicio publicador que simula la carga de datos en la base de datos y
envía notificaciones a través de RabbitMQ.
"""

import time
import uuid
import random
import logging
import argparse
import pika # type: ignore
from datetime import datetime

# Importar utilidades de RabbitMQ
from rabbitmq_utils import (
    RabbitMQConnection, 
    serialize_message, 
    EXCHANGE_NAME, 
    ROUTING_KEY
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Simulación de parámetros de calidad del agua
WATER_PARAMETERS = [
    "pH", "Turbidez", "Oxígeno disuelto", "Conductividad", 
    "Coliformes totales", "E. coli", "Nitratos", "Fosfatos",
    "Temperatura", "Sólidos suspendidos"
]

# Simulación de ubicaciones de muestreo
SAMPLE_LOCATIONS = [
    "Río Principal - Estación Norte", 
    "Río Principal - Estación Centro",
    "Río Principal - Estación Sur", 
    "Afluente Este", 
    "Afluente Oeste",
    "Laguna Central", 
    "Embalse Municipal"
]

# Simulación de entidades que reportan datos
REPORTING_ENTITIES = [
    "Secretaría de Medio Ambiente",
    "Instituto Hidrológico Municipal",
    "Universidad Autónoma - Facultad de Ingeniería Ambiental",
    "Empresa de Servicios Públicos",
    "ONG EcoAgua"
]

def simulate_data_upload():
    """
    Simula la carga de datos de calidad del agua generando
    valores aleatorios para los parámetros.
    """
    # Generar ID único para el lote de datos
    batch_id = str(uuid.uuid4())
    
    # Seleccionar ubicación y entidad aleatoria
    location = random.choice(SAMPLE_LOCATIONS)
    entity = random.choice(REPORTING_ENTITIES)
    
    # Generar entre 3 y 10 mediciones aleatorias
    num_measurements = random.randint(3, 10)
    measurements = []
    
    # Fecha y hora actual para la carga
    timestamp = datetime.now().isoformat()
    
    # Generar mediciones aleatorias
    for _ in range(num_measurements):
        parameter = random.choice(WATER_PARAMETERS)
        # Generar valor según el parámetro (simplificado)
        if parameter == "pH":
            value = round(random.uniform(6.0, 9.0), 2)
            unit = "pH"
        elif parameter == "Temperatura":
            value = round(random.uniform(15.0, 30.0), 1)
            unit = "°C"
        elif parameter == "Turbidez":
            value = round(random.uniform(0.5, 15.0), 2)
            unit = "NTU"
        elif parameter == "Oxígeno disuelto":
            value = round(random.uniform(2.0, 12.0), 2)
            unit = "mg/L"
        elif parameter in ["Coliformes totales", "E. coli"]:
            value = random.randint(0, 5000)
            unit = "UFC/100mL"
        else:
            # Para otros parámetros, valores genéricos
            value = round(random.uniform(0.1, 100.0), 2)
            unit = "mg/L"
            
        # Determinar si el valor está por encima del límite permisible (simulado)
        threshold_exceeded = random.random() < 0.2  # 20% de probabilidad
        
        measurements.append({
            "parameter": parameter,
            "value": value,
            "unit": unit,
            "threshold_exceeded": threshold_exceeded
        })
    
    # Componer los datos completos de la carga
    data = {
        "batch_id": batch_id,
        "timestamp": timestamp,
        "location": location,
        "reporting_entity": entity,
        "measurements": measurements,
        "metadata": {
            "device_id": f"SENSOR-{random.randint(1000, 9999)}",
            "upload_method": random.choice(["API", "Desktop App", "Field Device"]),
            "comments": "Datos simulados para prueba de concepto"
        }
    }
    
    # Registrar en log y retornar los datos
    logger.info(f"Simulada carga de datos: batch_id={batch_id}, ubicación={location}")
    return data

def publish_notification(rmq_connection, data):
    """
    Publica una notificación en RabbitMQ cuando se cargan datos.
    
    Args:
        rmq_connection: Conexión a RabbitMQ
        data: Datos a publicar en la notificación
    """
    try:
        # Preparar mensaje de notificación
        notification = {
            "event_type": "data_upload",
            "timestamp": datetime.now().isoformat(),
            "data": data
        }
        
        # Serializar mensaje
        message_body = serialize_message(notification)
        
        # Publicar mensaje en el exchange
        rmq_connection.channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensaje persistente
                content_type='application/json'
            )
        )
        
        logger.info(f"Notificación publicada: batch_id={data['batch_id']}")
        return True
    except Exception as e:
        logger.error(f"Error al publicar notificación: {e}")
        return False

def main():
    """Función principal que simula cargas periódicas de datos"""
    parser = argparse.ArgumentParser(description='Simulador de carga de datos para el Observatorio de Calidad del Agua')
    parser.add_argument('--host', default='localhost', help='Host de RabbitMQ')
    parser.add_argument('--port', type=int, default=5672, help='Puerto de RabbitMQ')
    parser.add_argument('--user', default='guest', help='Usuario de RabbitMQ')
    parser.add_argument('--password', default='guest', help='Contraseña de RabbitMQ')
    parser.add_argument('--interval', type=int, default=10, 
                        help='Intervalo en segundos entre cargas de datos (default: 10)')
    args = parser.parse_args()
    
    # Crear conexión a RabbitMQ
    rmq_connection = RabbitMQConnection(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password
    )
    
    try:
        # Establecer conexión
        if not rmq_connection.connect():
            logger.error("No se pudo conectar a RabbitMQ. Verifique que el servicio esté en ejecución.")
            return

        logger.info(f"Iniciando simulación de carga de datos cada {args.interval} segundos. Presione Ctrl+C para detener.")
        
        # Bucle principal
        while True:
            # Simular carga de datos
            data = simulate_data_upload()
            
            # Publicar notificación
            publish_notification(rmq_connection, data)
            
            # Esperar intervalo configurado
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        logger.info("Simulación interrumpida por el usuario")
    finally:
        # Cerrar conexión
        rmq_connection.close()

if __name__ == "__main__":
    main()
