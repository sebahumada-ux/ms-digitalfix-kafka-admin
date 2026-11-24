# DigitalFix - Microservicio Kafka Admin

El microservicio permite administrar tópicos, particiones, configuraciones y grupos de consumidores mediante una API REST. También permite publicar eventos de demostración para verificar el funcionamiento de los productores, consumidores y mecanismos de recuperación de mensajes fallidos.

## Integrantes

- Sebastián Ahumada
- Benjamín Gutiérrez

## Tecnologías utilizadas

- Java 21
- Spring Boot
- Spring Kafka
- Apache Kafka
- Apache ZooKeeper
- Maven
- Docker
- Docker Compose
- Kafka UI
- AWS EC2
- Caddy
- HTTPS

## Funcionalidades implementadas

El microservicio permite:

- Crear tópicos Kafka.
- Listar y consultar tópicos existentes.
- Consultar configuraciones de tópicos.
- Modificar configuraciones de retención y limpieza.
- Aumentar la cantidad de particiones.
- Eliminar tópicos con confirmación.
- Consultar grupos de consumidores.
- Revisar offsets y lag de consumidores.
- Publicar eventos de órdenes de trabajo.
- Generar mensajes inválidos para demostrar el manejo de errores.
- Verificar el procesamiento de mensajes mediante Kafka Analytics.
- Administrar recursos Kafka mediante una API REST.

## Arquitectura

Kafka Admin forma parte de la infraestructura de mensajería y procesamiento de eventos del proyecto DigitalFix.

```text
                  DigitalFix
                      |
         +------------+------------+
         |                         |
         v                         v
    Workorders                Kafka Admin
    Producer                     :8090
         |                         |
         +------------+------------+
                      |
                      v
               Apache Kafka
                      |
         +------------+------------+
         |            |            |
         v            v            v
      Broker 1     Broker 2     Broker 3
         |            |            |
         +------------+------------+
                      |
                      v
               Kafka Analytics
                    :8091
                      |
                      v
              Dashboard Angular
```

Kafka Admin administra los recursos del clúster y permite generar eventos de demostración.

Workorders publica eventos reales asociados a las operaciones de las órdenes de trabajo.

Kafka Analytics consume los eventos, los clasifica y genera métricas utilizadas por el dashboard de DigitalFix.

## Infraestructura Kafka

La infraestructura está compuesta por:

- 3 brokers Apache Kafka.
- 3 nodos Apache ZooKeeper.
- 1 interfaz Kafka UI.
- 1 microservicio Kafka Admin.
- 1 microservicio Kafka Analytics.

### Distribución de componentes

| Componente | Función |
|---|---|
| Kafka Broker 1 | Almacenamiento y procesamiento de eventos. |
| Kafka Broker 2 | Almacenamiento y replicación de eventos. |
| Kafka Broker 3 | Almacenamiento y replicación de eventos. |
| ZooKeeper 1 | Coordinación del clúster. |
| ZooKeeper 2 | Coordinación del clúster. |
| ZooKeeper 3 | Coordinación del clúster. |
| Kafka UI | Visualización y administración de recursos. |
| Kafka Admin | Administración mediante API REST. |
| Kafka Analytics | Consumo y procesamiento de eventos. |

Los brokers utilizan replicación para mantener copias de las particiones y permitir la continuidad del servicio ante determinados fallos individuales.

## Tópicos Kafka

La infraestructura de DigitalFix contempla cuatro tópicos principales y de recuperación.

### Tópicos principales

- `digitalfix.ordenes.eventos`
- `digitalfix.analitica.eventos`

El tópico `digitalfix.ordenes.eventos` almacena eventos relacionados con las órdenes de trabajo.

El tópico `digitalfix.analitica.eventos` almacena las métricas generadas por Kafka Analytics.

### Dead Letter Topics

- `digitalfix.ordenes.eventos.DLT`
- `digitalfix.analitica.eventos.DLT`

Los tópicos DLT reciben mensajes que no pudieron procesarse correctamente mediante los mecanismos de recuperación configurados en Kafka Analytics.

## Configuración de tópicos

Los cuatro tópicos utilizan la siguiente configuración inicial:

| Parámetro | Valor |
|---|---|
| Particiones | 3 |
| Factor de replicación | 3 |
| Réplicas mínimas sincronizadas | 2 |
| Retención | 7 días |
| Política de limpieza | delete |

La configuración inicial de los tópicos se encuentra en el repositorio de infraestructura:

`digitalfix-infra/kafka/scripts/init-topics.sh`

## Particiones y replicación

Las particiones permiten distribuir los mensajes dentro del clúster Kafka.

El factor de replicación permite mantener copias de las particiones en diferentes brokers.

Conceptos utilizados:

- **Partition:** división lógica de un tópico.
- **Leader:** réplica responsable de gestionar las operaciones de una partición.
- **Follower:** réplica que sincroniza la información del líder.
- **ISR:** conjunto de réplicas sincronizadas.
- **Offset:** posición de un mensaje dentro de una partición.
- **Key:** identificador utilizado para determinar la distribución de los mensajes.

Los eventos de órdenes utilizan el identificador de la orden como key, permitiendo que los mensajes asociados a una misma orden se mantengan en la misma partición mientras se conserve la configuración de particionado.

## Puerto

El microservicio utiliza el puerto:

`8090`

Durante la ejecución local, la API puede consultarse mediante:

`http://localhost:8090`

En AWS, el puerto se encuentra vinculado a localhost de la instancia EC2 y el acceso HTTPS externo se realiza mediante Caddy.

## Endpoints principales

### Administración de tópicos

```http
GET    /api/kafka/topics
GET    /api/kafka/topics/{nombre}
POST   /api/kafka/topics
PATCH  /api/kafka/topics/{nombre}/config
POST   /api/kafka/topics/{nombre}/partitions
DELETE /api/kafka/topics/{nombre}?confirmar=true
```

### Administración de consumer groups

```http
GET /api/kafka/groups
GET /api/kafka/groups/{grupo}/lag
```

### Publicación de eventos de demostración

```http
POST /api/demo/workorders
POST /api/demo/malformed-event
```

## Ejemplos de utilización

Los siguientes ejemplos utilizan Bash y se pueden ejecutar desde la instancia EC2 o desde un entorno local donde Kafka Admin esté disponible.

### Listar tópicos

```bash
curl http://localhost:8090/api/kafka/topics
```

Permite consultar los tópicos disponibles en el clúster.

### Consultar un tópico

```bash
curl http://localhost:8090/api/kafka/topics/digitalfix.ordenes.eventos
```

Permite consultar la información del tópico y sus particiones.

### Crear un tópico

```bash
curl -X POST http://localhost:8090/api/kafka/topics \
  -H "Content-Type: application/json" \
  -d '{
    "nombre": "demo.ev3",
    "particiones": 3,
    "replicas": 3,
    "retencionMs": 86400000,
    "limpieza": "delete"
  }'
```

La solicitud crea un tópico de demostración con tres particiones, tres réplicas y una retención de un día, siempre que el tópico no exista previamente.

### Consultar grupos de consumidores

```bash
curl http://localhost:8090/api/kafka/groups
```

Permite consultar los grupos de consumidores disponibles.

### Consultar lag de consumidores

```bash
curl http://localhost:8090/api/kafka/groups/digitalfix-analytics-workorders/lag
```

Permite revisar la diferencia entre los offsets procesados por el grupo y los offsets disponibles en Kafka.

### Publicar un evento de orden

```bash
curl -X POST http://localhost:8090/api/demo/workorders \
  -H "Content-Type: application/json" \
  -d '{
    "workOrderId": 555,
    "clienteId": 1,
    "estado": "CREADA"
  }'
```

El evento se publica en el tópico:

`digitalfix.ordenes.eventos`

Este endpoint permite demostrar el flujo de publicación y consumo de mensajes sin crear una orden real en Oracle Database.

### Publicar un mensaje inválido

```bash
curl -X POST http://localhost:8090/api/demo/malformed-event
```

Permite generar un mensaje inválido para verificar el manejo de errores y la recuperación mediante Dead Letter Topics.

El resultado debe verificarse mediante los logs de Kafka Analytics y Kafka UI.

## Consumer Groups

La infraestructura utiliza los siguientes grupos principales:

- `digitalfix-analytics-workorders`
- `digitalfix-analytics-metrics`

Kafka Admin permite consultar los grupos y revisar su estado mediante la API REST.

La administración de consumer groups facilita la supervisión de:

- Particiones asignadas.
- Offsets registrados.
- Mensajes disponibles.
- Lag de consumidores.

## Kafka UI

Kafka UI permite visualizar gráficamente la infraestructura Kafka.

Funcionalidades:

- Consultar brokers.
- Listar tópicos.
- Visualizar particiones.
- Revisar réplicas.
- Consultar ISR.
- Visualizar mensajes.
- Revisar keys.
- Consultar offsets.
- Visualizar consumer groups.
- Revisar lag de consumidores.

Puerto local:

`8088`

Dirección local:

`http://localhost:8088`

En el despliegue AWS, Kafka UI puede consultarse mediante el dominio HTTPS configurado en Caddy.

## Docker

El proyecto incluye un archivo:

`Dockerfile`

Este archivo permite construir la imagen Docker del microservicio utilizando Java 21.

La configuración Docker Compose se encuentra en:

`digitalfix-infra/kafka/docker-compose.yml`

Kafka Admin comparte la red Docker utilizada por la infraestructura Kafka.

Los servidores Kafka utilizados dentro de la red Docker son:

```text
kafka-1:9092
kafka-2:9092
kafka-3:9092
```

## Compilación

### Windows

```powershell
.\mvnw.cmd clean package
```

### Linux

```bash
./mvnw clean package
```

### Ejecución de pruebas

En Windows:

```powershell
.\mvnw.cmd test
```

En Linux:

```bash
./mvnw test
```

## Ejecución local

Con Kafka disponible y los servidores bootstrap correctamente configurados, ejecutar en Windows:

```powershell
.\mvnw.cmd spring-boot:run
```

En Linux:

```bash
./mvnw spring-boot:run
```

## Despliegue mediante Docker Compose

Los comandos se ejecutan desde el directorio padre que contiene los repositorios de DigitalFix.

### Iniciar infraestructura Kafka

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml up -d
```

### Inicializar tópicos

Una vez que los brokers estén operativos:

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile tools run --rm kafka-init
```

### Iniciar Kafka Admin y Kafka Analytics

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile apps up -d --build kafka-admin kafka-analytics
```

### Verificar contenedores

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile apps ps
```

### Consultar logs

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile apps logs --tail=50 kafka-admin
```

## Despliegue en AWS

Kafka Admin fue desplegado como parte de la infraestructura completa de DigitalFix en una instancia AWS EC2.

Características del entorno:

- Amazon Linux 2023.
- Instancia EC2 t3.large.
- Docker y Docker Compose.
- Tres brokers Kafka.
- Tres nodos ZooKeeper.
- Kafka UI.
- Redes Docker compartidas.
- Caddy como proxy inverso HTTPS.

### Arquitectura de acceso

```text
Usuario / Administrador
          |
          | HTTPS
          v
        Caddy
          |
          v
 Kafka Admin :8090
          |
          v
    Clúster Kafka
```

El acceso externo a Kafka Admin se protege mediante autenticación HTTP básica configurada en Caddy.

Los puertos internos de Kafka no se exponen directamente a Internet.

La guía completa de despliegue se encuentra en:

[DigitalFix - Despliegue completo en AWS](https://github.com/sebahumada-ux/digitalfix-infra/blob/main/aws/README.md)

## Seguridad

Consideraciones de seguridad:

- No publicar credenciales ni tokens en el repositorio.
- No exponer directamente los puertos de los brokers Kafka.
- Utilizar HTTPS para el acceso externo.
- Restringir el acceso a las herramientas administrativas.
- Mantener las configuraciones privadas fuera del control de versiones.
- Utilizar los endpoints de modificación y eliminación únicamente con usuarios autorizados.

Kafka Admin es una herramienta administrativa y no debe exponerse públicamente sin protección.

La autenticación HTTP básica de Caddy es independiente de la autorización JWT utilizada por los microservicios de negocio de DigitalFix.

## Validación del funcionamiento

Durante las pruebas de integración se verificó:

- Funcionamiento del clúster Kafka con tres brokers.
- Creación y consulta de tópicos.
- Configuración de particiones y réplicas.
- Publicación de eventos de demostración.
- Consulta de mensajes mediante Kafka UI.
- Integración con Kafka Analytics.
- Acceso a Kafka Admin mediante HTTPS en AWS.
- Consulta de los cuatro tópicos configurados en DigitalFix.

El despliegue integrado fue probado en AWS junto con Workorders, Catalog, BFF, RabbitMQ, Notify, Oracle Database y el frontend Angular.

## Consideraciones de disponibilidad

Los tres brokers Kafka se encuentran desplegados como contenedores dentro de una instancia EC2.

Esta configuración permite demostrar replicación y tolerancia ante fallos individuales de brokers, pero no garantiza disponibilidad ante la pérdida completa de la instancia.

Los datos de Kafka se almacenan mediante volúmenes Docker persistentes.

## Repositorios relacionados

### Infraestructura DigitalFix

https://github.com/sebahumada-ux/digitalfix-infra

### Kafka Analytics

https://github.com/sebahumada-ux/ms-digitalfix-kafka-analytics

### Workorders

https://github.com/sebahumada-ux/digitalfix-ms-workorders

### Frontend DigitalFix

https://github.com/sebahumada-ux/frontend-digitalfix

## Estado del proyecto

Actualmente se encuentran implementados:

- Administración REST de tópicos Kafka.
- Gestión de configuraciones y particiones.
- Consulta de consumer groups.
- Consulta de offsets y lag.
- Publicación de eventos de órdenes.
- Generación de mensajes inválidos de demostración.
- Integración con Kafka Analytics.
- Integración con Kafka UI.
- Configuración mediante Docker Compose.
- Despliegue en AWS EC2.
- Acceso mediante HTTPS.
- Código fuente y documentación publicados en GitHub.
