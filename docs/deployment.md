# Deployment

## Overview

```text
 sending processes                          ASGI workers
 (views, tasks, shell)                      (one hub per worker)

 broadcast()                                hub ──► open streams
     │ after commit                          ▲  │
     ▼                                       │  │
 ┌────────────────────────────────┐  NOTIFY  │  │
 │ PostgreSQL                     │──────────┘  │ SELECT … WHERE id = ANY
 │   INSERT the event             │◄────────────┘
 │   pg_notify('pg_sse', …)       │
 └────────────────────────────────┘
```

`broadcast()` stores each event and sends a `NOTIFY` with its id and topic, in one statement.
An ASGI worker starts one hub with its first stream, and the hub listens on the channel.
A hub with a stream on that topic loads the event from the table and writes it to its streams.
A reconnecting browser replays missed events from the table.
Any process that can write to the database can send; only the ASGI workers serve streams.

## ASGI server

Streams need an ASGI server.
`manage.py runserver` speaks WSGI and cannot stream an async response.

Pick one of them.

granian:

```sh
granian --interface asginl --workers 4 --workers-kill-timeout 10 \
    myproject.asgi:application
```

uvicorn:

```sh
uvicorn --workers 4 --timeout-graceful-shutdown 10 myproject.asgi:application
```

daphne:

```sh
daphne myproject.asgi:application
```

Each worker process runs one event loop.
Each event loop runs one hub.

### Shutdown

A healthy stream stays open while its client stays connected.
granian and uvicorn wait for open responses when they stop, so a single open tab holds a restart.
Both wait without limit by default.
Give them a limit:

- granian: `--workers-kill-timeout 10`, or `GRANIAN_WORKERS_KILL_TIMEOUT`.
- uvicorn: `--timeout-graceful-shutdown 10`.

The browsers reconnect to the new workers and replay what they missed.

granian's `--workers-lifetime` restarts a worker after a set time, which ends the worker's streams.

### Concurrent streams

Every open stream is a request that does not finish.
granian handles at most `--backpressure` requests per worker at once.
Its default is the backlog of 1024 divided by the workers.
So one worker holds 1024 streams, and each of four workers holds 256.
Further connections wait, and a client may give up before it gets a turn.
Raise the limit for many open tabs, such as `--backpressure 8192` or `GRANIAN_BACKPRESSURE=8192`.
Each stream also holds a file descriptor, so `ulimit -n` must allow as many.
In the load test below, each stream from 1000 to 2000 added about 140 KB to the worker.
Django runs each request's sync code in its own thread, and a stream keeps that thread until it ends.

### Load test

The [demo chat](https://github.com/oliverandrich/django-pg-sse-demo#load-test) ships a load test script.
Each client joins with a name, so presence tracks it, and opens one stream.
A sender then posts 10 messages per second, and every client measures each message's delay.
These runs used one granian worker with `--backpressure 8192`, on one MacBook with a local PostgreSQL:

| Streams | Connected after | Lost | Delay p50 / p99 | Worker RSS |
|---|---|---|---|---|
| 1000 | 10.5 s | 0 | 28 / 45 ms | 222 MB |
| 2000 | 21.2 s | 0 | 46 / 103 ms | 364 MB |

The clients arrived at 100 per second, so both runs connected as fast as they arrived.

The demo's presence receiver sends at most one online list per second, and the list has a fixed size.
A receiver that sends all names on every join does work that grows with the square of the readers.
With such a receiver, 2000 clients needed 90 seconds to connect, and the worker grew to 834 MB.

## Database

### Connections

Each worker holds its own connections to the alias in `PG_SSE["DATABASE"]`, outside Django's connection pool.
They are one for `LISTEN`, one for live delivery and a pool of up to `PG_SSE["STREAM_CONNECTIONS"]` (default 4).
An open stream holds no connection from Django's pool.
Presence queries share the stream pool, so they add no connections.

For 4 workers with the defaults, the hubs hold up to 4 × (2 + 4) = 24 connections.
Django's own pool adds up to its `max_size` per worker.
Add both when you set `max_connections`.

[Database connections](index.md#database-connections) lists which settings and `OPTIONS` the hub uses.

### Django's connection pool

Django's documentation advises a connection pool under ASGI in place of persistent connections.
Each request's sync code runs in a thread of its own, so `CONN_MAX_AGE` reuses nothing.
Signal receivers, such as presence, run their sync code in asgiref's shared sync thread.
`django-pg-sse` closes that thread's connections to `default` and the hub's alias after each signal.
With Django's pool, closing returns a connection to the pool.
Without one, each signal and each request opens a new PostgreSQL connection.

Turn the pool on for both aliases:

```python
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "app",
        "OPTIONS": {"pool": True},
    },
}
```

The pool needs `psycopg[pool]`, and `CONN_MAX_AGE` must stay `0`.
`manage.py check --deploy` warns with `pg_sse.W002` for `default` and `pg_sse.W003` for the hub's alias.
A PgBouncer in transaction mode pools for `default`; silence `pg_sse.W002` in `SILENCED_SYSTEM_CHECKS` then.
A hub alias that bypasses PgBouncer still needs Django's pool, which `pg_sse.W003` reports.

### Timeouts

The hub's connections add libpq defaults for `connect_timeout`, keepalives and `tcp_user_timeout`.
[Settings](settings.md#databases) lists them; a key in `OPTIONS` replaces its default.
`PG_SSE["QUERY_TIMEOUT"]` (default 30 seconds) limits each query on the live connection and the stream pool.
The `LISTEN` health check has its own limit of 10 seconds.
Raise `QUERY_TIMEOUT` for a database that answers slowly under load.

### PgBouncer

`LISTEN` needs a session.
A PgBouncer in transaction mode cannot carry it.
A PgBouncer in session mode keeps one server connection per client connection, so `LISTEN` works.

With a PgBouncer in transaction mode, give the hub its own alias that bypasses it:

```python
DATABASES = {
    "default": {  # requests, through PgBouncer
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "pgbouncer",
        "PORT": 6432,
        "NAME": "app",
    },
    "direct": {  # the same database, without PgBouncer
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "postgres",
        "PORT": 5432,
        "NAME": "app",
        "OPTIONS": {"pool": True},
    },
}
PG_SSE = {"DATABASE": "direct"}
```

The hub's `LISTEN`, live and stream connections then use `direct`.
`broadcast()`, the stream view and `prune_stream_events` keep using `default`.
`NOTIFY` works through PgBouncer in transaction mode, because PostgreSQL sends it at commit.
Both aliases must reach the same database.
Once the hub listens, it compares `current_database()` and the cluster's `system_identifier` through both.
It logs an error at `django_pg_sse.hub` if they differ.
If the comparison cannot finish, it logs a warning and runs again after the next reconnect.
A clone of the same cluster, such as a promoted replica, passes the comparison.

## Reverse proxy

Each stream sends a comment line after 15 quiet seconds, so proxy idle timeouts do not cut it.
The stream also sends `X-Accel-Buffering: no` and Django's never-cache headers.

### Caddy

```text
example.com {
    encode
    reverse_proxy 127.0.0.1:8000
}
```

Caddy's `reverse_proxy` flushes `text/event-stream` responses at once, whatever `flush_interval` says.
Its `encode` directive streams such responses as they are written, according to the Caddy docs.

### nginx

```text
location /events/ {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
}
```

nginx honours `X-Accel-Buffering: no`, so the location needs no `proxy_buffering off;`.
Its default `proxy_read_timeout` of 60 seconds exceeds the 15-second keepalive.

### HTTP/2

Over HTTP/1.1, a browser opens at most six connections to one origin.
Each tab with a stream holds one of them, so further requests wait once six tabs are open.
Serve the site over HTTP/2, which carries all requests to an origin on one connection.
Caddy speaks HTTP/2 over HTTPS by default.
nginx 1.25.1 and later need `http2 on;` in the `server` block.

## Failures

| What happens | What the browser sees |
|---|---|
| A worker restarts | The stream ends. htmx reconnects to another worker and replays what it missed. |
| The hub loses its `LISTEN` connection | The hub reconnects after 0.5 to 5 seconds and replays what it missed into its open streams. |
| The `LISTEN` connection goes half-open | After 30 quiet seconds the hub checks it and replaces it about 10 to 20 seconds later. |
| A hub query hangs | It gives up after `QUERY_TIMEOUT` plus up to 10 seconds. If the server did not answer, the query runs once more on a new connection; otherwise the listener reconnects. |
| A stream's query fails | The stream ends with an error, which the ASGI server logs, and the browser reconnects. |
| The hub does not listen for 60 seconds | A new stream ends, and the browser reconnects, perhaps to another worker. |
| The database comes back with lower ids | The hub follows the table on its next reconnect; a browser's newer id resumes nothing. |
| A stream's client falls 1000 events behind | The stream ends, and the reconnect replays the dropped events. |

## Pruning

Every event is stored.
`manage.py prune_stream_events` deletes events older than `PG_SSE["RETENTION_HOURS"]` (default 24).
Run it regularly, for example hourly.

With cron:

```text
17 * * * * cd /srv/app && .venv/bin/python manage.py prune_stream_events -v 0
```

With a systemd timer:

```ini
# prune-stream-events.service
[Service]
Type=oneshot
WorkingDirectory=/srv/app
ExecStart=/srv/app/.venv/bin/python manage.py prune_stream_events -v 0

# prune-stream-events.timer
[Timer]
OnCalendar=hourly

[Install]
WantedBy=timers.target
```

With django-crontask:

```python
from crontask import cron
from django.core.management import call_command
from django.tasks import task


@cron("17 * * * *")
@task
def prune_stream_events():
    call_command("prune_stream_events", verbosity=0)
```

## Monitoring

| Logger | Level | Event |
|---|---|---|
| `django_pg_sse.hub` | WARNING | The listener lost its connection and reconnects. |
| `django_pg_sse.hub` | ERROR | The listener failed for another reason and restarts. |
| `django_pg_sse.hub` | WARNING | A NOTIFY on the channel had a payload the hub cannot read. |
| `django_pg_sse.hub` | WARNING | The newest event id fell, such as after a restore. |
| `django.db.backends.base` | ERROR | Storing an event failed after the commit, and the event is lost. |
| `django_pg_sse.hub` | ERROR | `PG_SSE["DATABASE"]` reaches another database than `default`. |
| `django_pg_sse.hub` | WARNING | The hub could not compare its database with `default`. |
| `django.dispatch` | ERROR | A receiver of `stream_opened`, `stream_closed` or `presence_changed` raised. |
| `django_pg_sse.presence.tracker` | WARNING | A presence refresh or sweep failed; the next one retries. |

A failed query of an opening stream ends that stream; the ASGI server logs its traceback.

`manage.py check` runs the system checks `pg_sse.E001` to `pg_sse.E005` and `pg_sse.W001`.
`manage.py check --deploy` adds `pg_sse.W002` and `pg_sse.W003`, which warn about a database without Django's pool.
The signals `stream_opened` and `stream_closed` can count open streams, for example as a gauge:

```python
from django.dispatch import receiver
from django_pg_sse.signals import stream_closed, stream_opened


@receiver(stream_opened)
def count_open(sender, stream, **kwargs):
    open_streams.inc()


@receiver(stream_closed)
def count_closed(sender, stream, **kwargs):
    open_streams.dec()
```

A killed process sends no `stream_closed`, so such a gauge drifts until the process restarts.

## Security

`Last-Event-ID` values are signed with `SECRET_KEY`.
An authenticated stream's ids are signed for its user, its session and its topics.
An anonymous stream's ids are signed for its topics only.
A changed `SECRET_KEY` invalidates the ids that browsers hold, and their next reconnect replays nothing.
With the old key in `SECRET_KEY_FALLBACKS`, those ids keep resuming.

The table stores each event's data as sent, such as HTML with private content.
`RETENTION_HOURS` therefore also limits how long that content stays in the database.
Close events store unsalted SHA-256 digests of user primary keys and session keys.
A session key cannot be read back from its digest.
An integer primary key can, by hashing every candidate.

Topics starting with `_` belong to the package.
The stream view refuses them, and `broadcast()` raises `ValueError` for them.

`broadcast()` sends its data unchanged, so HTML from user input must be escaped.
`render_to_string()` escapes template variables by default.
