# Broker demo: Kafka vs RabbitMQ vs Redis Pub/Sub

Very small full-stack demo showing:

- **Kafka**: messages **persist** (consumer can catch up later)
- **RabbitMQ**: messages are **queued + acked** (when consumer is down, they **requeue**)
- **Redis Pub/Sub**: messages are **ephemeral** (published while subscriber is down are **lost**)

## Prereqs

- Docker + Docker Compose
- Java 21
- Maven (`mvn`)

## Start brokers

From the project root:

```bash
docker compose up -d
```

## Run the Spring Boot app

```bash
mvn spring-boot:run
```

Open the UI:

- `http://localhost:8080`

## How to demo the differences

1. Send a few messages with consumer **enabled** (everything is received immediately).
2. Toggle **Consumer enabled** OFF.
3. While OFF, send messages to each broker:
   - **Kafka**: app will not ack offsets, so messages will be consumed when you turn the consumer back ON.
   - **RabbitMQ**: consumer rejects with requeue, so the queue retains messages until consumer is ON.
   - **Redis**: subscriber unsubscribes while OFF, so messages published during OFF are lost.
4. Toggle consumer ON again and observe which messages show up.

## Notes

- No database: logs are stored in an in-memory list (`/logs`).
- Kafka topic/queue/channel names are in `application.yml` (`demo.*`).
- RabbitMQ management UI: `http://localhost:15672` (user/pass: `guest` / `guest`).

