# Разбор кода Audit Service

Этот документ объясняет проект сверху вниз: от запуска инфраструктуры до
обработки Kafka-сообщения. Он дополняет README и предназначен для знакомства с
кодом перед защитой проекта.

## 1. Общий поток данных

Создание события:

```text
HTTP POST /api/audit
    -> handler разбирает JSON
    -> service проверяет бизнес-правила
    -> repository сохраняет audit_log
    -> producer отправляет событие в Kafka
    -> handler возвращает event_id и timestamp
```

Периодическая аналитика:

```text
Kafka user-actions
    -> consumer группы analytics-group
    -> repository пересчитывает последний час
    -> транзакционно обновляется stats_cache
    -> consumer отмечает сообщения и коммитит offsets
```

Ручной replay:

```text
POST /api/admin/rebuild-stats
    -> replayer находит offsets по датам
    -> отдельно читает каждую партицию
    -> считает действия
    -> заменяет содержимое stats_cache
```

## 2. Корневые файлы

### `go.mod` и `go.sum`

`go.mod` задаёт имя модуля `audit-service`, версию Go и прямые зависимости.
`go.sum` хранит контрольные суммы зависимостей и обычно не редактируется
вручную.

Основные библиотеки:

- Sarama — Kafka producer и consumer group;
- pgx — PostgreSQL и пул соединений;
- UUID — идентификаторы событий;
- Testcontainers — Kafka для интеграционного теста.

### `.env.example`

Содержит пример переменных окружения. Настоящие пароли не должны попадать в
репозиторий. Приложение использует значения окружения или безопасные локальные
значения по умолчанию.

### `.gitignore`

Исключает настройки IDE, `.env`, кэш, логи и результаты сборки. Эти файлы
зависят от компьютера разработчика и не являются исходным кодом.

### `README.md`

Главная пользовательская инструкция: назначение проекта, запуск, API, тесты и
полезные команды.

## 3. `docker-compose.yml`

Compose описывает инфраструктуру:

- `kafka` — единственный Kafka-брокер в режиме KRaft;
- `kafka-init` — после healthcheck создаёт `user-actions` с тремя партициями;
- `postgres` — PostgreSQL с постоянным volume;
- `postgres-init` — после healthcheck применяет SQL-миграцию.

Init-контейнеры являются одноразовыми. Состояние `Exited (0)` означает, что
команда успешно выполнена.

Kafka имеет два listener:

- внешний нужен Go-приложению на хосте;
- внутренний нужен контейнерам в Docker-сети.

## 4. `cmd/audit-service/main.go`

`package main` и функция `main()` образуют исполняемую программу.

Основные блоки:

1. Создаётся JSON-логгер `slog`.
2. `config.Load()` читает настройки.
3. `signal.NotifyContext` создаёт общий контекст остановки по `Ctrl+C` или
   `SIGTERM`.
4. `pgxpool.New` создаёт пул PostgreSQL, а `Ping` проверяет соединение.
5. Создаются producer, repository, service, replayer, handler и consumer.
6. HTTP-сервер и consumer запускаются в отдельных goroutine.
7. Главная goroutine ожидает `<-ctx.Done()`.
8. `server.Shutdown` выполняет graceful shutdown.

`main.go` называется точкой сборки приложения: он связывает компоненты, но не
содержит их бизнес-логику.

`defer` регистрирует действие, которое выполнится перед выходом из текущей
функции. Здесь он используется для закрытия пула, producer, consumer и
replayer.

## 5. `internal/config/config.go`

`Config` перечисляет настраиваемые параметры:

- адрес HTTP;
- PostgreSQL DSN;
- Kafka brokers, topic и group ID;
- размер пакета;
- интервалы auto commit и аналитики;
- время graceful shutdown.

`Load()` преобразует строки окружения в `int` и `time.Duration` и проверяет,
что значения положительные. Функция `env()` возвращает переменную окружения или
значение по умолчанию.

## 6. `internal/domain/event.go`

Domain содержит общие структуры, не зависящие от HTTP, Kafka или PostgreSQL:

- `ValidActions` — множество `login`, `view`, `purchase`;
- `Event` — полное событие;
- `HistoryFilter` — фильтры и пагинация;
- `Stat` — группа и количество;
- `ReplayResult` — ответ ручного replay.

Один общий тип `Event` используется service, repository, producer, consumer и
replayer.

## 7. `internal/handler/http.go`

Handler переводит HTTP в вызовы Go-кода.

### Маршруты

`Routes()` регистрирует healthcheck, четыре API-метода и Swagger. В Go 1.22
строки вида `"POST /api/audit"` одновременно задают метод и путь.

### Создание события

`createAudit()`:

1. ограничивает body одним мегабайтом;
2. включает `DisallowUnknownFields`;
3. декодирует ровно один JSON-объект;
4. вызывает `AuditService.CreateEvent`;
5. преобразует validation error в HTTP 400;
6. возвращает HTTP 201 с UUID и временем.

Второй вызов `Decode` должен вернуть `io.EOF`. Это доказывает, что после
первого объекта в body остались только пробелы, а не второй JSON.

### История и статистика

`auditHistory()` разбирает `user_id`, действие, даты, страницу и limit.
`auditStats()` принимает `group_by=action|day`.
`rebuildStats()` разбирает период и вызывает независимый replayer.

Вспомогательные функции централизуют разбор дат, положительных чисел и запись
JSON-ответов.

## 8. `internal/service/audit.go`

Service содержит бизнес-правила и не знает деталей HTTP.

Интерфейсы `EventRepository` и `EventProducer` описывают только необходимые
методы. Благодаря этому service можно тестировать заглушками.

`CreateEvent()`:

1. очищает строковые поля от пробелов;
2. проверяет обязательные поля и action;
3. превращает отсутствующий `meta` в `{}`;
4. проверяет, что `meta` является JSON-объектом;
5. генерирует UUID и UTC timestamp;
6. сохраняет событие;
7. синхронно отправляет его в Kafka;
8. при ошибке Kafka пытается удалить запись из БД.

Компенсационное удаление уменьшает риск рассинхронизации, но не даёт полной
атомарности между двумя системами. Production-улучшение — Transactional
Outbox.

`History()` и `Stats()` валидируют фильтры перед передачей в repository.

## 9. `internal/repository/audit.go`

Repository изолирует SQL от остальных слоёв.

- `SaveEvent` вставляет строку в `audit_log`;
- `DeleteEvent` выполняет компенсацию после ошибки Kafka;
- `History` строит условия фильтрации и использует `LIMIT/OFFSET`;
- `Stats` группирует строки по action или календарному дню;
- `RefreshStatsCache` пересчитывает настоящий последний час из `audit_log`;
- `ReplaceStatsCache` сохраняет результат replay.

Обновление `stats_cache` выполняется в транзакции:

```text
BEGIN -> DELETE -> INSERT -> COMMIT
```

Если вставка не удалась, deferred `Rollback` оставляет старые данные без
частичного обновления.

## 10. `internal/producer/producer.go`

Producer использует `sarama.SyncProducer`.

Важные настройки:

- `WaitForAll` ожидает подтверждение Kafka;
- retries повторяют временно неудачную отправку;
- hash partitioner выбирает партицию по ключу.

`SendEvent()` кодирует событие в JSON и отправляет его с ключом `user_id`.
Одинаковый ключ получает одинаковый hash, поэтому события пользователя
попадают в одну партицию и сохраняют порядок.

## 11. `internal/consumer/consumer.go`

Consumer подключается к группе `analytics-group`.

Sarama вызывает:

- `Setup` после назначения партиций;
- `Cleanup` перед их отзывом;
- `ConsumeClaim` для чтения назначенной партиции.

`ConsumeClaim` логирует key, partition и offset и добавляет сообщения в
`pending`. Flush запускается по размеру пакета, таймеру, закрытию claim или
rebalance.

`flush()` сначала транзакционно обновляет `stats_cache`. Только после успеха он
вызывает:

```text
MarkMessage -> Commit
```

Поэтому auto commit не может подтвердить ещё не отмеченное сообщение. Если
процесс завершится до commit, сообщение будет прочитано повторно — это
семантика at-least-once.

Committed offset указывает на следующее сообщение. После обработки сообщения с
offset 6 сохранённая позиция становится 7.

## 12. `internal/replay/replay.go`

Replayer использует обычный Sarama consumer без group ID.

Для каждой партиции он:

1. получает offset первого сообщения не раньше `from`;
2. получает границу `to` или конец лога;
3. создаёт partition consumer с начального offset;
4. читает до конечной границы;
5. считает действия.

Такой consumer не участвует в `analytics-group`, поэтому replay не изменяет её
committed offsets.

## 13. `migrations/001_init.sql`

Миграция создаёт:

- `audit_log` с UUID, пользователем, действием, ресурсом, JSONB и временем;
- индексы для истории пользователя и фильтра action;
- `stats_cache` для агрегатов.

Ограничения `NOT NULL`, `CHECK` и `PRIMARY KEY` защищают данные даже в случае
ошибки приложения.

`CREATE ... IF NOT EXISTS` делает повторный запуск миграции безопасным.

## 14. Swagger

`internal/handler/swagger/openapi.yaml` является контрактом API: описывает
маршруты, параметры, схемы и коды ответов.

`index.html` загружает Swagger UI. Директива `//go:embed swagger/*` встраивает
оба файла в Go-бинарник.

## 15. `tests/integration/kafka_test.go`

Build tag `integration` отделяет медленный Docker-тест от обычных тестов.

Тест:

1. запускает временную Kafka;
2. создаёт топик с тремя партициями;
3. отправляет событие через настоящий `Producer`;
4. читает его consumer’ом;
5. проверяет key и поля события;
6. автоматически удаляет контейнер.

Это проверяет реальное прохождение сообщения, а не mock Kafka.

## 16. Что помнить на защите

```text
handler = HTTP
service = правила
repository = SQL
producer = запись в Kafka
consumer = групповая обработка и offsets
replayer = независимое повторное чтение
main = сборка и жизненный цикл
```

Ключевые гарантии:

- одинаковый `user_id` сохраняет порядок внутри одной партиции;
- offset подтверждается только после успешной критической записи;
- до commit возможен повтор, но не потеря сообщения;
- replay не перемещает offsets основной группы;
- PostgreSQL удобен для запросов, Kafka — для журнала и повторного чтения.
