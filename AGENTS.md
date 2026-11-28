# AGENTS.md

Guidance for coding agents and contributors. Development setup and commit conventions are in
[CONTRIBUTING.md](CONTRIBUTING.md).

## What this project is

`django-pg-sse` pushes server-sent events from Django to browsers over PostgreSQL
`LISTEN`/`NOTIFY`. It stores every event, so a reconnecting client replays what it missed.

## Package layout

```
src/django_pg_sse/
├── messages.py      # SSE format, NOTIFY payloads, Message, Owner, Signal, EventIds; Django only for signing
├── publish.py       # broadcast(), broadcast_json(), close_streams(), their async variants, the logout receiver
├── hub.py           # one listener per event loop; fans events out to the loop's streams
├── views.py         # the SSE endpoint and its authorisation
├── signals.py       # stream_opened, stream_closed, StreamInfo; asend() and close_connection()
├── permissions.py   # the default can_subscribe() policy
├── conf.py          # the PG_SSE setting and its defaults
├── checks.py        # system checks E001 to E005, W001 middleware, W002/W003 pool (--deploy)
├── models.py        # StreamEvent, the stored events
├── management/commands/prune_stream_events.py
└── presence/        # optional app django_pg_sse.presence, label django_pg_sse_presence
    ├── __init__.py  # Presence, presence_changed, user_identity; present() and apresent() lazily
    ├── tracker.py   # one Tracker per hub: join, leave, heartbeat, sweeps; present()
    ├── models.py    # PresenceEntry in the UNLOGGED table django_pg_sse_presence
    ├── checks.py    # E006 PRESENCE_TOPICS, E007 PRESENCE_IDENTITY
    └── apps.py      # registers the checks and the stream signal receivers
```

## Invariants

- `Hub._listen()` reads the newest id before `LISTEN` on first start, so no commit falls between.
  `_catch_up()` replays up to the newest id and then sets `last_id` to it, read topics or not.
  A stream subscribing meanwhile then sees `last_id` past `authorized_at` and checks for closes.
- A LISTEN connection quiet for `HEALTH_SECONDS` must answer `SELECT 1` within `HEALTH_TIMEOUT`.
  Otherwise `_listen()` treats it as lost and reconnects.
  psycopg then waits up to 10 seconds more for the cancelled query before the connection closes.
- `_within()` turns a timeout into `OperationalError`, so callers treat it like a lost connection.
  `_fetch()` and `pool.check()` run within `PG_SSE["QUERY_TIMEOUT"]`; `_check()` within `HEALTH_TIMEOUT`.
  psycopg closes a connection whose server does not answer the cancel, so the retry below applies.
- The hub's connections use `PG_SSE["DATABASE"]` through `hub_database()`.
  `broadcast()`, the stream view and `prune_stream_events` use `default`.
  `_verify_database()` runs as a task after `ready.set()`, within `QUERY_TIMEOUT`.
  It logs an error if both reach different databases, and runs again after a reconnect if it could not compare.
- `connect_params()` sets keepalives and `tcp_user_timeout` unless `OPTIONS` sets them.
  It sets `connect_timeout` only without `PGCONNECT_TIMEOUT`, `PGSERVICE` and `OPTIONS["service"]`.
- `_Connection.fetch()` and `StreamPool.fetch()` retry once when an `OperationalError` closed the connection.
  `StreamPool.fetch()` first runs `check()`, which drops every closed idle connection of the pool.
  A failed connect and other errors, such as SQL errors, are not retried.
  `StreamPool.transaction()` does not retry; each of its queries runs within `QUERY_TIMEOUT`.
- `broadcast()` writes after the commit, in a short transaction holding an advisory lock.
  Event ids therefore become visible in order. Replay after `Last-Event-ID` depends on it.
- `_replay()` skips an event with a `latest` key when a newer event has the same topic and key.
  The stream's replay and the hub's catch-up both use `_replay()`, so both skip replaced snapshots.
  Delivery by NOTIFY sends every event.
  The column has a database default, so an older release's INSERT without it keeps working during a deploy.
- `_publish()` runs one statement: it takes the lock, stores the event and sends the NOTIFY.
  The INSERT reads from the lock's CTE, so the event takes its id under the lock.
- Topics starting with `_` are reserved. The view refuses them before `CAN_SUBSCRIBE` runs,
  and `broadcast()` raises `ValueError`. Package code writes them through `_broadcast()`.
- `close_streams()` stores an event on each close topic of the owner, `_close:<kind>:<sha256>`.
  The hub indexes close topics like any topic, so a listener that reconnects still applies them.
- The digest keeps the session key out of the table and fits any primary key into the column.
- `Hub._listen()` passes every NOTIFY already received to `_receive()` at once.
  `_receive()` looks up every payload id with one `id = ANY` query.
  It loads data only for topics it reads, again for streams that subscribed meanwhile.
  It delivers without an await in between and advances `last_id` only afterwards.
- Any role that can connect to the database can NOTIFY the channel, so a payload proves nothing.
  `_receive()` takes topics from the table, and `last_id` counts only stored ids.
  A close therefore needs a stored row, and a forged id neither halts delivery nor raises `authorized_at`.
- An open stream holds no connection from Django's pool. The hub uses connections of its own.
  `Hub.live` runs the queries of live delivery and of the catch-up.
  `Hub.streams` pools up to `STREAM_CONNECTIONS` for the view's replay and revocation queries.
  `views._messages()` closes the request thread's connection after the response middleware.
- The hub's connections bypass Django's connection setup, so `_connect()` applies `assume_role`.
  `connect_params()` uses `get_connection_params()`, which drops `pool`, `isolation_level` and
  `server_side_binding`. It also drops Django's `cursor_factory` and `context`.
- A stream sends event ids signed by `EventIds` for its owner's close topics and its topics.
  Replay needs a validly signed id and no close for the owner after that id.
- `views._with_keepalives()` wraps `_messages()` and sends a keepalive after `KEEPALIVE_SECONDS` without output.
  `_messages()` therefore has no keepalive logic of its own; it may wait on anything.
- `stream_opened` and `stream_closed` come in pairs: `_messages()` sends closed once opened began.
  The closed send runs under `asyncio.shield`, so a second cancellation cannot cut it short.
  `signals.asend()` runs receivers through `asend_robust()`, so a failing receiver is logged and the stream goes on.
  It then closes the request thread's connection again, which a sync receiver may have opened.
- `StreamInfo.request` is the request that opened the stream. Its session and user load lazily,
  so receivers read them in sync code.
- `Hub.stop()` awaits each callback in `Hub.on_stop`, then closes its connections even if one raised.
  A stopped pool refuses to reopen. `get_hub()` would start a new hub with a pool nothing closes,
  so code that runs after a stream ended calls `get_hub(create=False)`.
- `Hub.extensions` holds state of optional apps for the hub's lifetime, keyed by app name.
- Presence: a `Tracker` lives in `hub.extensions["django_pg_sse.presence"]` and registers its `stop()` in `Hub.on_stop`.
  `stream_closed` looks it up through `get_hub(create=False)`, so a late close starts no hub.
- `Tracker.stop()` marks every stream it tracks closed, so other workers report the leaves after `GRACE_SECONDS`.
  A join that resumes after the stop finds `stopped` set and writes nothing.
- Presence changes for one topic and identity hold an advisory lock, taken in one statement in key order.
  So two workers never both report a join or a leave, and a hash collision cannot deadlock.
- `Tracker.join()` registers the stream before it writes, so a cancellation after the commit still leaves.
- The heartbeat refreshes existing rows with a plain `UPDATE`.
  Only rows it did not find go through the locked `_write()`, which reports their readers as joined.
- `stream_closed` only marks rows closed, batched per loop turn. One pending sweep reports the leaves
  after `GRACE_SECONDS`, so a reconnect in that time changes nothing.
- A sweep deletes rows closed past `GRACE_SECONDS` or without a heartbeat for `EXPIRE_SECONDS`.
  `present()` skips the same rows through `GONE`, even before a sweep runs.
- `present()` reads through `hub_database()` and bypasses routers: a standby refuses UNLOGGED tables.
- An identity is visible to every reader of the topic, so it must never contain a session key.
- `__init__.py` must not import the model at module level. Django imports the package while it
  loads `INSTALLED_APPS`, before models can load.

## Docs

`docs/` builds with Sphinx, MyST and furo on Read the Docs.
`docs/index.md` includes the README.
`docs/changelog.md` includes CHANGELOG.md.
`docs/api.md` lists the public API for autodoc; add a new public name there.
`docs/settings.md` lists every `PG_SSE` key; `tests/test_docs.py` fails when one is missing.
`mise run build-docs` builds with `-W`, as the CI job does.
`mise run serve-docs` rebuilds on change.
`tests/test_docs.py` fails when a module or an exported name lacks a docstring.

## Tests

The suite needs a PostgreSQL server; `tests/settings.py` reads `POSTGRES_*` variables and
defaults to the local Unix socket. Hub tests run in the test's own event loop: decorate them with
`stops_the_hub`, because Django's test cases run no `asyncTearDown`. `Hub.stop()` also stops the
presence tracker.
Keep a reference to every stream a test opens: the garbage collector finalises a dropped stream at a
random moment, which closes it and sends `stream_closed`.
