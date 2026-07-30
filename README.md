# Audit Service

Микросервис аудита пользовательских действий на Go, PostgreSQL и Apache Kafka.

Сервис принимает события `login`, `view` и `purchase`, хранит их в PostgreSQL,
публикует в Kafka и поддерживает агрегаты за последний час.

## Поток события

```text
POST /api/audit
        |
        v
PostgreSQL transaction
  ├── audit_log
  └── outbox_events
        |
        v
outbox publisher
        |
        v
Kafka: user-actions (3 partitions, key=user_id)
        |
        v
analytics-group
  ├── analytics_events
  └── stats_cache

Некорректное сообщение ──> Kafka: user-actions-dlq
```

`audit_log` и `outbox_events` создаются в одной транзакции. Поэтому событие не
может сохраниться без задачи на публикацию. Publisher повторяет неуспешные
отправки, что даёт доставку at-least-once.

Consumer сохраняет прочитанные Kafka-события в `analytics_events`, обновляет
`stats_cache` и только после успешной транзакции коммитит offsets. Повторная
доставка безопасна: `analytics_events.event_id` является первичным ключом.

Ручной replay использует независимый consumer без group ID. Он возвращает
исторический результат в HTTP-ответе, не изменяет offsets `analytics-group` и не
перезаписывает текущий часовой `stats_cache`.

## Запуск

Требуются Go 1.25+ и Docker Desktop.

```powershell
Copy-Item .env.example .env
docker compose up -d
go run ./cmd/audit-service
```

Проверка:

```powershell
Invoke-RestMethod http://localhost:8080/health
```

Swagger UI: <http://localhost:8080/swagger/>

PostgreSQL доступен на хосте через порт `5433`, Kafka — через `9092`.
Контейнеры `kafka-init`, `kafka-dlq-init` и `postgres-init` завершаются с кодом
`0` после создания топиков и применения миграции.

## API

| Метод | Маршрут | Назначение |
|---|---|---|
| `POST` | `/api/audit` | Записать пользовательское действие |
| `GET` | `/api/audit` | История с фильтрацией и пагинацией |
| `GET` | `/api/stats` | Статистика пользователя по `action` или `day` |
| `POST` | `/api/admin/rebuild-stats` | Replay Kafka за выбранный период |
| `GET` | `/health` | Проверка сервиса |

Пример события:

```json
{
  "user_id": "user-1",
  "action": "view",
  "resource_id": "product-42",
  "meta": {
    "source": "catalog"
  }
}
```

Для `/api/stats` параметры `user_id` и `group_by` обязательны.

## Конфигурация

Основные переменные:

| Переменная | Значение по умолчанию |
|---|---|
| `HTTP_ADDR` | `:8080` |
| `HTTP_READ_TIMEOUT` | `10s` |
| `HTTP_WRITE_TIMEOUT` | `15s` |
| `HTTP_IDLE_TIMEOUT` | `60s` |
| `DATABASE_URL` | `postgres://audit:audit@127.0.0.1:5433/audit?sslmode=disable` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `user-actions` |
| `KAFKA_DLQ_TOPIC` | `user-actions-dlq` |
| `KAFKA_GROUP_ID` | `analytics-group` |
| `KAFKA_BATCH_SIZE` | `100` |
| `KAFKA_PRODUCER_TIMEOUT` | `10s` |
| `KAFKA_COMMIT_INTERVAL` | `5s` |
| `ANALYTICS_INTERVAL` | `5m` |
| `OUTBOX_INTERVAL` | `1s` |
| `OUTBOX_BATCH_SIZE` | `100` |
| `SHUTDOWN_PERIOD` | `10s` |

Docker Compose также читает `POSTGRES_DB`, `POSTGRES_USER`,
`POSTGRES_PASSWORD` и `POSTGRES_HOST_PORT` из `.env`.

## Тесты

Unit-тесты:

```powershell
go test ./...
```

Полный integration flow через Testcontainers:

```powershell
go test -tags=integration ./tests/integration -v -count=1
```

Integration-тест поднимает временные Kafka и PostgreSQL и проверяет цепочку:

```text
AuditService -> outbox -> producer -> consumer group
             -> analytics_events -> stats_cache -> committed offset
```

Подробное объяснение реализации: [docs/code-walkthrough.md](docs/code-walkthrough.md).
