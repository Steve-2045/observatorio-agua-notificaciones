#!/usr/bin/env python3
"""
Servicio consumidor que recibe y procesa las notificaciones de carga de datos
para administradores del Observatorio de Calidad del Agua.
"""

import os
import time
import signal
import logging
import argparse
import threading
from datetime import datetime

# Importar utilidades de RabbitMQ
from rabbitmq_utils import (
    RabbitMQConnection, 
    deserialize_message, 
    EXCHANGE_NAME, 
    QUEUE_NAME, 
    ROUTING_KEY
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Banderas para control de la aplicación
shutdown_flag = threading.Event()

class ConsoleNotifier:
    """
    Clase para mostrar notificaciones en la consola con formato
    y colores para mejor visualización.
    """
    # Códigos ANSI para colores en terminal
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    
    def __init__(self):
        """Inicializa el contador de notificaciones"""
        self.notification_count = 0
    
    def clear_screen(self):
        """Limpia la pantalla de la consola"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def print_header(self):
        """Imprime el encabezado de la aplicación"""
        self.clear_screen()
        print(f"{self.BOLD}{self.BLUE}======================================================{self.RESET}")
        print(f"{self.BOLD}{self.BLUE}  OBSERVATORIO DE DATOS DE CALIDAD DEL AGUA{self.RESET}")
        print(f"{self.BOLD}{self.BLUE}  Sistema de Notificaciones para Administradores{self.RESET}")
        print(f"{self.BOLD}{self.BLUE}======================================================{self.RESET}")
        print(f"Conectado a RabbitMQ - Escuchando notificaciones...")
        print(f"Notificaciones recibidas: {self.notification_count}")
        print(f"{self.BOLD}{self.BLUE}======================================================{self.RESET}\n")
    
    def display_notification(self, data):
        """
        Muestra una notificación con formato en la consola
        
        Args:
            data: Datos de la notificación deserializada
        """
        self.notification_count += 1
        self.print_header()
        
        # Extraer datos relevantes
        batch_id = data['data']['batch_id']
        timestamp = data['data']['timestamp']
        location = data['data']['location']
        entity = data['data']['reporting_entity']
        measurements = data['data']['measurements']
        
        # Formatear y mostrar notificación
        print(f"{self.BOLD}NUEVA CARGA DE DATOS DETECTADA{self.RESET}")
        print(f"{self.BOLD}ID del lote:{self.RESET} {batch_id}")
        print(f"{self.BOLD}Fecha y hora:{self.RESET} {timestamp}")
        print(f"{self.BOLD}Ubicación:{self.RESET} {location}")
        print(f"{self.BOLD}Entidad informante:{self.RESET} {entity}")
        print(f"\n{self.BOLD}Mediciones:{self.RESET}")
        
        # Mostrar mediciones con colores según estado
        for i, measurement in enumerate(measurements, 1):
            param = measurement['parameter']
            value = measurement['value']
            unit = measurement['unit']
            exceeded = measurement['threshold_exceeded']
            
            # Color según si excede el umbral
            status_color = self.RED if exceeded else self.GREEN
            status_text = "ALERTA" if exceeded else "Normal"
            
            print(f"  {i}. {param}: {status_color}{value} {unit}{self.RESET} - Estado: {status_color}{status_text}{self.RESET}")
        
        # Mostrar alerta si hay mediciones que exceden umbrales
        alerts = [m for m in measurements if m['threshold_exceeded']]
        if alerts:
            print(f"\n{self.BOLD}{self.RED}¡ATENCIÓN! {len(alerts)} parámetro(s) exceden los límites permitidos.{self.RESET}")
            print(f"{self.YELLOW}Se requiere revisión por parte del administrador.{self.RESET}")
        
        print(f"\n{self.BOLD}{self.BLUE}======================================================{self.RESET}")
        print(f"Esperando nuevas notificaciones... (Ctrl+C para salir)")

def process_message(ch, method, properties, body, notifier):
    """
    Callback para procesar mensajes recibidos de RabbitMQ
    
    Args:
        ch: Canal de RabbitMQ
        method: Información del método
        properties: Propiedades del mensaje
        body: Cuerpo del mensaje
        notifier: Instancia de ConsoleNotifier
    """
    # Deserializar mensaje
    data = deserialize_message(body)
    
    # Procesar y mostrar notificación
    logger.debug(f"Mensaje recibido: {data}")
    notifier.display_notification(data)
    
    # Confirmar recepción del mensaje
    ch.basic_ack(delivery_tag=method.delivery_tag)

def setup_consumer(rmq_connection):
    """
    Configura el consumidor para recibir notificaciones
    
    Args:
        rmq_connection: Conexión a RabbitMQ
    
    Returns:
        Instancia de ConsoleNotifier
    """
    # Crear cola para el consumidor y enlazarla al exchange
    rmq_connection.channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True  # Persistente ante reinicios
    )
    
    # Vincular cola al exchange con la routing key
    rmq_connection.channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=QUEUE_NAME,
        routing_key=ROUTING_KEY
    )
    
    # Crear instancia del notificador
    notifier = ConsoleNotifier()
    notifier.print_header()
    
    # Configurar callback con el notificador
    callback = lambda ch, method, properties, body: process_message(
        ch, method, properties, body, notifier
    )
    
    # Configurar consumidor
    rmq_connection.channel.basic_qos(prefetch_count=1)
    rmq_connection.channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback
    )
    
    return notifier

def signal_handler(sig, frame):
    """Manejador de señales para detener gracefully"""
    logger.info("Señal de interrupción recibida, cerrando...")
    shutdown_flag.set()

def main():
    """Función principal del consumidor de notificaciones"""
    parser = argparse.ArgumentParser(description='Consumidor de notificaciones para administradores del Observatorio')
    parser.add_argument('--host', default='localhost', help='Host de RabbitMQ')
    parser.add_argument('--port', type=int, default=5672, help='Puerto de RabbitMQ')
    parser.add_argument('--user', default='guest', help='Usuario de RabbitMQ')
    parser.add_argument('--password', default='guest', help='Contraseña de RabbitMQ')
    args = parser.parse_args()
    
    # Registrar manejador de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
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
        
        # Configurar consumidor
        notifier = setup_consumer(rmq_connection)
        
        # Iniciar consumo de mensajes en un hilo separado
        def consume_messages():
            try:
                logger.info("Iniciando consumo de mensajes...")
                rmq_connection.channel.start_consuming()
            except Exception as e:
                logger.error(f"Error en consumo de mensajes: {e}")
            finally:
                if rmq_connection.channel and rmq_connection.channel.is_open:
                    rmq_connection.channel.stop_consuming()
        
        consumer_thread = threading.Thread(target=consume_messages)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Esperar señal de cierre
        while not shutdown_flag.is_set():
            time.sleep(0.1)  # Pequeña pausa para evitar uso excesivo de CPU
            
    except Exception as e:
        logger.error(f"Error en el consumidor: {e}")
    finally:
        # Detener consumo y cerrar conexión
        if rmq_connection.channel and rmq_connection.channel.is_open:
            rmq_connection.channel.stop_consuming()
        rmq_connection.close()
        logger.info("Consumidor finalizado")

if __name__ == "__main__":
    main()
