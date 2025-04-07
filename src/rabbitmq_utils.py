#!/usr/bin/env python3
"""
Utilidad para gestionar conexiones a RabbitMQ para el sistema de notificaciones
del Observatorio de Datos de Calidad del Agua.
"""

import os
import json
import logging
import pika # type: ignore
from pika.exceptions import AMQPConnectionError # type: ignore

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constantes para la configuración
EXCHANGE_NAME = 'observatorio_events'
QUEUE_NAME = 'admin_notifications'
ROUTING_KEY = 'water.data.uploaded'

class RabbitMQConnection:
    """Clase para gestionar la conexión con RabbitMQ"""
    
    def __init__(self, host='localhost', port=5672, 
                 user='guest', password='guest', virtual_host='/'):
        """Inicializar la conexión a RabbitMQ"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.virtual_host = virtual_host
        self.connection = None
        self.channel = None
    
    def connect(self):
        """Establecer conexión con RabbitMQ"""
        try:
            # Credenciales de conexión
            credentials = pika.PlainCredentials(self.user, self.password)
            # Parámetros de conexión
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                heartbeat=600  # Mantener conexión activa
            )
            # Establecer conexión
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declarar exchange de tipo 'topic' para enrutamiento flexible
            self.channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type='topic',
                durable=True  # Persistente ante reinicios
            )
            
            logger.info(f"Conectado a RabbitMQ en {self.host}:{self.port}")
            return True
        except AMQPConnectionError as e:
            logger.error(f"Error al conectar a RabbitMQ: {e}")
            return False
    
    def close(self):
        """Cerrar la conexión a RabbitMQ"""
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("Conexión a RabbitMQ cerrada")

# Funciones de ayuda para serializar/deserializar mensajes
def serialize_message(data):
    """Convertir datos a formato JSON para enviar como mensaje"""
    try:
        return json.dumps(data).encode('utf-8')
    except Exception as e:
        logger.error(f"Error al serializar mensaje: {e}")
        return json.dumps({"error": "Error de serialización"}).encode('utf-8')

def deserialize_message(body):
    """Convertir mensaje recibido de bytes a objeto Python"""
    try:
        return json.loads(body.decode('utf-8'))
    except Exception as e:
        logger.error(f"Error al deserializar mensaje: {e}")
        return {"error": "Error al deserializar mensaje"}
