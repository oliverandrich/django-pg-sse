# Changelog

## Unreleased

### 📚 Documentation
- The README states the supported PostgreSQL versions, 15 to 19. CI tests 15, 18 and 19.

## 0.4.0 (2026-11-18)

### 🐛 Bug Fixes
- **Receivers run their sync code outside the stream's thread**: this applies to `stream_opened`, `stream_closed` and `presence_changed`.
  Before, a task a receiver started ran its sync code in the stream's thread.
  That thread kept a connection until the browser left.
  Enough open streams emptied Django's pool, and other requests failed with `PoolTimeout`.
  Under an ASGI server, receivers and their tasks now use asgiref's shared sync thread.
  They keep the request's language and script prefix.
- **`signals.close_connection()` also closes `PG_SSE["DATABASE"]`**, which `present()` reads through.
  It leaves a connection in an atomic block open.

### 🚀 Features
- **System checks `pg_sse.W002` and `pg_sse.W003`** warn under `check --deploy` when `default` or the hub's alias has no connection pool.

### 📚 Documentation
- The deployment guide and the README recommend Django's connection pool under ASGI and explain the cost without it.
- The deployment guide explains granian's backpressure, which limits the open streams per worker.
- The deployment guide shows load test results with 1000 and 2000 streams on one worker.

## 0.3.0 (2026-11-17)

### 🚀 Features
- **`presence_changed` reports `updated`**: a reader whose new stream carries a changed meta, such as a new name.
  `present()` shows that newest meta in the reader's first place.
- **`latest` for snapshot events**: `broadcast()`, `broadcast_json()` and their async variants take a key.
  A replay skips a snapshot when a newer one has the same topic and key.
  A returning tab therefore shows no outdated states.
  The migration adds the column `latest` with a database default and builds its index concurrently.
  Workers of 0.2 keep writing events during the deploy.

### 📚 Documentation
- The README and the presence page explain how htmx's pause of hidden tabs affects streams and presence.

## 0.2.0 (2026-11-17)

### 🚀 Features
- **Presence**: the optional app `django_pg_sse.presence` tracks who has a topic open.
  `present()` and `apresent()` list the readers; the `presence_changed` signal reports joins and leaves.
  `PG_SSE["PRESENCE_TOPICS"]` selects the tracked topics, and `PG_SSE["PRESENCE_IDENTITY"]` names each reader.
- **`StreamInfo.request`** gives receivers of `stream_opened` and `stream_closed` the request that opened the stream.

### 📚 Documentation
- **Presence** has its own page, and the settings page lists `PRESENCE_TOPICS` and `PRESENCE_IDENTITY`.
- The README guards `beforeend` appends against an element the page already shows, instead of relying on stable ids.

## 0.1.0 (2026-11-16)

### 🚀 Features
- **`broadcast(topic, data, event="")`** sends HTML, or any text in a named event, to the open server-sent event streams of a topic after the transaction commits.
- **`broadcast_json(topic, value, event=...)`** sends JSON in a named event for client code.
- **`abroadcast()`, `abroadcast_json()` and `aclose_streams()`** do the same from async code.
- **One stream per page** for several topics, with `Last-Event-ID` replay from stored events.
  Event ids are signed for the stream's user, session and topics, so a client cannot pick its own replay start.
- **`close_streams(user=..., session=...)`** ends streams after access is taken away; logging out closes the streams of that session.
- **`stream_opened` and `stream_closed` signals** report the lifecycle of each stream, such as for presence or metrics.
- **System checks** for PostgreSQL, the `PG_SSE` values and the authentication middleware.
- **`PG_SSE["DATABASE"]`** gives the hub its own alias, such as one that bypasses a PgBouncer in transaction mode.
- **Timeouts** for the hub's queries (`QUERY_TIMEOUT`, 30 s) and connects (10 s), and `tcp_user_timeout` on Linux, so a failover holds a worker for a bounded time.
- **Keepalives** after 15 quiet seconds, also while a stream opens, and the `X-Accel-Buffering: no` header for nginx.
- **Reserved topics**: the stream view and `broadcast()` refuse topics starting with `_`.
- **`prune_stream_events`** deletes events older than the retention.
- **Documentation** on Read the Docs, with settings, a deployment guide and an API reference.
