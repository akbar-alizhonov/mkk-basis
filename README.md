# Payments Service

Микросервис асинхронной обработки платежей: принимает платёж, проводит его через эмулятор
платёжного шлюза и уведомляет клиента через webhook.

**Стек:** FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL, RabbitMQ (FastStream),
Alembic, Docker Compose, [uv](https://docs.astral.sh/uv/).

## Быстрый старт

```bash
cp .env.example .env          # по желанию поменять API_KEY и т.д.
docker compose up -d --build
```

Поднимаются сервисы:

| Сервис             | Назначение                                                        | Порт          |
|--------------------|-------------------------------------------------------------------|---------------|
| `postgres`         | БД                                                                | 5432          |
| `rabbitmq`         | брокер + management UI (guest/guest)                              | 5672, 15672   |
| `migrations`       | одноразовый `alembic upgrade head`, остальные ждут его завершения | —             |
| `api`              | REST API, Swagger: http://localhost:8000/docs                     | 8000          |
| `outbox-relay`     | публикует события из таблицы `outbox` в RabbitMQ                  | —             |
| `consumer`         | обработчик очереди `payments.new`                                 | —             |
| `webhook-receiver` | демо-приёмник webhook'ов (только для проверки)                    | 8001          |

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key` (по умолчанию `dev-api-key`).

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{
        "amount": "100.50",
        "currency": "RUB",
        "description": "Заказ #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:8001/webhook"
      }'
```

```
HTTP/1.1 202 Accepted
{"payment_id":"3c99f267-...","status":"pending","created_at":"2026-10-07T18:49:58.301825Z"}
```

- `Idempotency-Key` обязателен (иначе `422`).
- Повтор с тем же ключом и тем же телом → тот же ответ `202` и заголовок `Idempotent-Replayed: true`,
  новый платёж не создаётся.
- Тот же ключ с другим телом → `409 Conflict`.
- `amount > 0`, не более 2 знаков после запятой; `currency` ∈ `RUB | USD | EUR`.

### Получение платежа

```bash
curl -H 'X-API-Key: dev-api-key' http://localhost:8000/api/v1/payments/<payment_id>
```

```json
{
  "id": "3c99f267-...",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Заказ #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-receiver:8001/webhook",
  "created_at": "2026-10-07T18:49:58.301825Z",
  "processed_at": "2026-10-07T18:50:02.179928Z"
}
```

### Webhook

После обработки consumer отправляет `POST` на `webhook_url`:

```json
{
  "event": "payment.succeeded",
  "payment_id": "3c99f267-...",
  "status": "succeeded",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Заказ #42",
  "metadata": {"order_id": 42},
  "created_at": "...",
  "processed_at": "..."
}
```

Заголовки `X-Payment-Id` и `X-Event` помогают получателю дедуплицировать уведомления
(доставка at-least-once). Любой ответ не 2xx или сетевая ошибка считаются неудачей.

Полученные демо-приёмником webhook'и: `curl http://localhost:8001/webhook`.

## Как это работает

```
POST /payments ──► [ payments + outbox ] одна транзакция
                          │
                   outbox-relay (FOR UPDATE SKIP LOCKED, publisher confirms)
                          │
                          ▼
   exchange "payments" ──payments.new──► queue payments.new ──► consumer
                                                                   │ 1. эмуляция шлюза 2–5 с (90% / 10%)
                                                                   │ 2. UPDATE status (только если pending)
                                                                   │ 3. POST webhook
                         ┌──── ошибка, попытка N < 3 ──────────────┤
                         ▼                                         │ ошибка на 3-й попытке
         queue payments.new.retry.N (TTL = 2·2^(N-1) с)            ▼
                         │ TTL истёк → DLX         exchange "payments.dlx" ──► queue payments.new.dlq
                         └──► exchange "payments" ──► payments.new
```

### Outbox pattern
Платёж и событие `payment.created` записываются в одной транзакции (`app/services.py`), поэтому
принятый платёж не может «потерять» событие. `outbox-relay` (`app/outbox/relay.py`) раз в секунду
забирает пачку неопубликованных событий с `SELECT ... FOR UPDATE SKIP LOCKED` (можно запускать
несколько реплик), публикует с publisher confirms и `mandatory` (неротуемое сообщение — ошибка,
а не тихая потеря) и проставляет `published_at`. При недоступности брокера событие остаётся в
таблице (`attempts`, `last_error`) и уходит, когда брокер вернётся. Гарантия — at-least-once.

### Идемпотентность
- **API:** уникальный индекс на `idempotency_key` + хэш тела запроса (`request_hash`). Гонка
  параллельных запросов с одним ключом решается уникальным ограничением: проигравший получает
  `IntegrityError`, откатывается и возвращает уже созданный платёж.
- **Consumer:** шлюз вызывается только для платежа в статусе `pending`, статус обновляется условным
  `UPDATE ... WHERE status = 'pending'`. Дубль сообщения или повтор после ошибки webhook'а не
  списывает деньги повторно, а только переотправляет уведомление.

### Retry и DLQ
- 3 попытки (`MAX_ATTEMPTS`) с экспоненциальной задержкой `RETRY_BASE_DELAY · 2^(N-1)`: 2 с, 4 с.
- Задержка реализована средствами RabbitMQ: отдельная очередь на каждый шаг с `x-message-ttl` и
  dead-letter обратно в `payments.new`. Consumer не блокируется на ожидании, а отдельные очереди
  исключают head-of-line blocking, который бывает при per-message TTL в одной очереди.
- Номер попытки передаётся в заголовке `x-attempt`, последняя ошибка — в `x-last-error`.
- После последней неудачи сообщение публикуется в `payments.new.dlq`. Туда же через
  `x-dead-letter-exchange` основной очереди попадают сообщения, которые не удалось распарсить
  (reject), и события для несуществующего платежа.
- Неуспех самого платежа (10% случаев) — это бизнес-результат (`failed`), а не ошибка: он не
  ретраится, клиент получает webhook `payment.failed`.

Сообщения в DLQ видно в RabbitMQ UI: http://localhost:15672 → Queues → `payments.new.dlq`.

## Проверка сценариев

```bash
# webhook, который всегда отвечает 500 → 3 попытки → DLQ
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' -H 'Content-Type: application/json' -H 'Idempotency-Key: fail-1' \
  -d '{"amount":"10","currency":"USD","description":"dlq demo","webhook_url":"http://webhook-receiver:8001/webhook/fail"}'

docker compose logs -f consumer
docker compose exec rabbitmq rabbitmqctl list_queues name messages

# Outbox: событие не теряется при недоступном брокере
docker compose stop rabbitmq
curl -X POST ... -H 'Idempotency-Key: outbox-1' ...   # 202, событие лежит в outbox
docker compose start rabbitmq                          # relay дошлёт событие, consumer обработает
```

## Конфигурация

Переменные окружения (`app/config.py`):

| Переменная             | По умолчанию   | Описание                                        |
|------------------------|----------------|-------------------------------------------------|
| `DATABASE_URL`         | localhost      | `postgresql+asyncpg://...`                      |
| `RABBITMQ_URL`         | localhost      | `amqp://...`                                    |
| `API_KEY`              | `dev-api-key`  | статический ключ для `X-API-Key`                |
| `MAX_ATTEMPTS`         | `3`            | попыток обработки до DLQ                        |
| `RETRY_BASE_DELAY`     | `2`            | базовая задержка retry, с                       |
| `WEBHOOK_TIMEOUT`      | `5`            | таймаут HTTP-запроса webhook'а, с               |
| `OUTBOX_POLL_INTERVAL` | `1`            | период опроса outbox, с                         |
| `OUTBOX_BATCH_SIZE`    | `100`          | событий за одну итерацию relay                  |
| `CONSUMER_PREFETCH`    | `10`           | сколько сообщений consumer обрабатывает параллельно |
| `GATEWAY_*`            | 2 / 5 / 0.9    | задержка и доля успеха эмулятора шлюза          |

## Локальная разработка (uv)

```bash
uv sync                                   # зависимости, включая dev
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn app.api.main:app --reload
uv run python -m app.outbox.relay
uv run python -m app.consumer.main
```

Тесты используют реальный PostgreSQL (БД `payments_test` создаётся автоматически):

```bash
docker compose up -d postgres
uv run pytest
uv run ruff check . && uv run ruff format --check .
```

## Структура

```
app/
  api/            FastAPI: приложение, роуты, зависимости (API key, сессия БД)
  consumer/       FastStream consumer, процессор платежа, эмулятор шлюза, отправка webhook
  outbox/relay.py публикация событий из outbox
  dev/            демо-приёмник webhook'ов
  config.py       настройки (pydantic-settings)
  models.py       SQLAlchemy-модели payments и outbox
  schemas.py      Pydantic-схемы API и сообщений
  services.py     создание платежа: идемпотентность + запись в outbox
  messaging.py    топология RabbitMQ (exchanges, очереди, retry, DLQ)
migrations/       Alembic
tests/            pytest
```
