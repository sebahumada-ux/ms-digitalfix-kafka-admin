# DigitalFix - Microservicio Kafka Admin

Microservicio encargado de la administración de Apache Kafka dentro del proyecto DigitalFix.

## Integrantes

- Sebastián Ahumada
- Benjamín Gutiérrez

## Tecnologías utilizadas

- Java 21
- Spring Boot
- Spring Kafka
- Maven
- Apache Kafka
- Apache ZooKeeper
- Docker
- Docker Compose
- Kafka UI

## Funcionalidades implementadas

El microservicio permite:

- Crear tópicos Kafka.
- Listar y consultar tópicos existentes.
- Modificar configuraciones de retención y limpieza.
- Aumentar particiones.
- Eliminar tópicos con confirmación.
- Consultar grupos de consumidores.
- Revisar el lag de consumidores.
- Publicar eventos de órdenes de trabajo.
- Generar mensajes inválidos para demostrar el manejo de errores y DLT.

## Infraestructura Kafka

La infraestructura utiliza:

- 3 brokers Kafka.
- 3 nodos ZooKeeper.
- 1 interfaz Kafka UI.

Tópicos principales:

- `digitalfix.ordenes.eventos`
- `digitalfix.analitica.eventos`

La configuración de los tópicos principales contempla 3 particiones y 3 réplicas.

## Puerto

El microservicio utiliza el puerto:

`8090`

## Endpoints principales

```http
GET    /api/kafka/topics
GET    /api/kafka/topics/{nombre}
POST   /api/kafka/topics
PATCH  /api/kafka/topics/{nombre}/config
POST   /api/kafka/topics/{nombre}/partitions
DELETE /api/kafka/topics/{nombre}?confirmar=true

GET    /api/kafka/groups
GET    /api/kafka/groups/{grupo}/lag

POST   /api/demo/workorders
POST   /api/demo/malformed-event
```

## Arquitectura

```text
Kafka Admin :8090
       |
       v
Apache Kafka
       |
       +---- Broker 1
       +---- Broker 2
       +---- Broker 3
       |
       v
Kafka Analytics :8091
```

## Docker

El proyecto incluye un archivo:

`Dockerfile`

La infraestructura Kafka se ejecuta mediante Docker Compose, utilizando tres brokers, tres nodos ZooKeeper y Kafka UI.

## Compilación

En Windows:

```powershell
.\mvnw.cmd clean package
```

Para ejecutar las pruebas:

```powershell
.\mvnw.cmd test
```

## Ejecución local

Con Kafka disponible, en Windows:

```powershell
.\mvnw.cmd spring-boot:run
```

## Estado del proyecto

Actualmente se encuentran implementados:

- Administración REST de tópicos Kafka.
- Gestión de configuraciones y particiones.
- Consulta de grupos de consumidores.
- Publicación de eventos de órdenes.
- Generación de mensajes de prueba.
- Integración con la infraestructura Kafka.
- Compilación mediante Maven.
- Código fuente publicado en GitHub.

## Proyecto académico

**Asignatura:** Desarrollo Cloud Native I

**Evaluación:** EV3 - Apache Kafka

**Institución:** Duoc UC
