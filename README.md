# Audit Service

Микросервис аудита пользовательских действий на Go, PostgreSQL и Apache Kafka.
События сохраняются в `audit_log` и публикуются в Kafka с ключом `user_id`,
чтобы действия одного пользователя попадали в одну партицию и сохраняли порядок.

## Возможности

- запись действий `login`, `view` и `purchase`;
- история пользователя с фильтрацией и пагинацией;
- статистика по действиям или UTC-дням;
- периодическое обновление `stats_cache` consumer-группой `analytics-group`;
- ручной replay событий Kafka за выбранный период;
- Swagger/OpenAPI и интеграционный тест с Testcontainers.

## Архитектура

```text
POST /api/audit
    ├── PostgreSQL: audit_log
    └── Kafka: user-actions (3 partitions, key=user_id)
                           │
                           ▼
                    analytics-group
                           │
                           ▼
                     stats_cache

GET  /api/audit ──────────────── PostgreSQL
GET  /api/stats ──────────────── PostgreSQL
POST /api/admin/rebuild-stats ── Kafka replay ── stats_cache
```

Kafka работает в режиме KRaft без ZooKeeper. Docker Compose поднимает Kafka и
PostgreSQL, создаёт топик `user-actions` с тремя партициями и применяет миграцию.
Go-приложение запускается отдельно на хосте.

## Быстрый запуск

Требуются Go 1.25+, Docker Desktop и свободные порты `8080`, `9092`, `5433`.

```bash
docker compose up -d
docker compose ps -a
go run ./cmd/audit-service
```

`kafka` и `postgres` должны иметь статус `healthy`. Контейнеры `kafka-init` и
`postgres-init` выполняются один раз, поэтому для них нормален статус `Exited (0)`.

Проверка сервиса:

```bash
curl http://localhost:8080/health
```

Swagger UI: <http://localhost:8080/swagger/>

OpenAPI: <http://localhost:8080/swagger/openapi.yaml>

PostgreSQL доступен на хосте через порт `5433`; внутри Docker используется
стандартный порт `5432`.

## API

| Метод | Маршрут | Назначение |
|---|---|---|
| `POST` | `/api/audit` | Сохранить событие в PostgreSQL и отправить в Kafka |
| `GET` | `/api/audit` | Получить историю пользователя с фильтрами и пагинацией |
| `GET` | `/api/stats` | Сгруппировать события по `action` или `day` |
| `POST` | `/api/admin/rebuild-stats` | Пересчитать статистику через Kafka replay |
| `GET` | `/health` | Проверить доступность сервиса |

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

Поля `user_id`, `action` и `resource_id` обязательны. `meta` необязательно и по
умолчанию равно `{}`. Полные схемы запросов и ответов находятся в Swagger.

## Конфигурация

| Переменная | Значение по умолчанию |
|---|---|
| `HTTP_ADDR` | `:8080` |
| `DATABASE_URL` | `postgres://audit:audit@127.0.0.1:5433/audit?sslmode=disable` |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `user-actions` |
| `KAFKA_GROUP_ID` | `analytics-group` |
| `KAFKA_BATCH_SIZE` | `100` |
| `KAFKA_COMMIT_INTERVAL` | `5s` |
| `ANALYTICS_INTERVAL` | `5m` |
| `SHUTDOWN_PERIOD` | `10s` |

Пример значений находится в `.env.example`. Приложение читает переменные
окружения; файл `.env` автоматически не загружается.

## Тесты

Обычные тесты:

```bash
go test ./...
```

Интеграционный тест producer → Kafka → consumer:

```bash
go test -tags=integration ./tests/integration -v -count=1
```

Интеграционный тест через Testcontainers запускает временную Kafka, создаёт
топик с тремя партициями и проверяет ключ и содержимое доставленного события.

## Документация кода

Подробный разбор файлов, offsets, rebalance и replay:
[docs/code-walkthrough.md](docs/code-walkthrough.md).

## Остановка

```bash
docker compose down
```

Команда сохраняет PostgreSQL volume. `docker compose down -v` также удаляет
volume и все локальные данные проекта.
