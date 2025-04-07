# Sistema de Notificaciones del Observatorio de Datos de Calidad del Agua

Este proyecto implementa un sistema de notificaciones en tiempo real basado en el patrón arquitectónico Publicador-Suscriptor utilizando RabbitMQ como intermediario de mensajes.

## Descripción

El sistema permite la comunicación asíncrona entre los componentes que generan eventos (publicadores) cuando se cargan nuevos datos de calidad del agua y los componentes que procesan dichos eventos (suscriptores) para notificar a los administradores, especialmente cuando hay parámetros que exceden los umbrales permitidos.

## Estructura del Proyecto

```
sistema-notificaciones/
├── src/                  # Código fuente
│   ├── admin_consumer.py    # Consumidor de notificaciones para administradores
│   ├── data_publisher.py    # Publicador que simula la carga de datos
│   ├── rabbitmq_utils.py    # Utilidades compartidas para RabbitMQ
├── docs/                 # Documentación
│   ├── Sistema de Notificaciones con RabbitMQ - Documentación Técnica.pdf
├── README.md             # Este archivo
├── requirements.txt      # Dependencias del proyecto
├── .gitignore            # Archivos y directorios ignorados por Git
```

## Requisitos

- Python 3.6+
- RabbitMQ Server 3.8+
- Dependencias listadas en `requirements.txt`

## Instalación

1. Clonar el repositorio:
```bash
git clone https://github.com/tu-usuario/sistema-notificaciones.git
cd sistema-notificaciones
```

2. Crear y activar un entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

4. Asegurarse de que RabbitMQ esté en ejecución:
```bash
# Verificar estado en Linux
sudo systemctl status rabbitmq-server

# Iniciar si es necesario
sudo systemctl start rabbitmq-server
```

## Uso

1. Iniciar el consumidor de notificaciones (en una terminal):
```bash
python src/admin_consumer.py
```

2. Iniciar el publicador de datos simulados (en otra terminal):
```bash
python src/data_publisher.py --interval 5  # Simula carga de datos cada 5 segundos
```

## Contribuyentes

- Johan Stiven Avila Garcia - @Steve-2045
- Angie Katherine Gutierrez Varon - @AKGV-22

## Licencia

Este proyecto está licenciado bajo MIT.
