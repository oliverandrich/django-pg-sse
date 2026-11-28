import asyncio
import contextlib
import functools
import hashlib
import os
import signal
import subprocess
import threading
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any
from unittest import mock

import psycopg
from asgiref.sync import ThreadSensitiveContext, sync_to_async
from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.auth.models import AnonymousUser, User
from django.contrib.sessions.backends.db import SessionStore
from django.core.exceptions import SynchronousOnlyOperation
from django.db import DEFAULT_DB_ALIAS, connection, connections, transaction
from django.http import HttpRequest, HttpResponseBase, StreamingHttpResponse
from django.test import AsyncClient, RequestFactory, SimpleTestCase, TransactionTestCase, override_settings
from django.utils import translation
from psycopg import sql

from django_pg_sse import hub as hub_module
from django_pg_sse import messages, permissions, publish, signals, views
from django_pg_sse.models import StreamEvent

type Stream = AsyncIterator[bytes]


class CanSubscribeTest(SimpleTestCase):
    def check(self, user: AbstractBaseUser | AnonymousUser, topic: str) -> bool:
        request = RequestFactory().get("/events/")
        request.user = user
        return permissions.can_subscribe(request, topic)

    def test_anyone_may_read_public_topics(self):
        self.assertTrue(self.check(AnonymousUser(), "public:news"))

    def test_a_user_topic_belongs_to_that_user_alone(self):
        user = User(pk=7)
        self.assertTrue(self.check(user, "user:7"))
        self.assertFalse(self.check(user, "user:8"))
        self.assertFalse(self.check(AnonymousUser(), "user:7"))

    def test_other_topics_are_closed_until_the_project_opens_them(self):
        self.assertFalse(self.check(User(pk=7), "room:1"))


def allow_every_topic(request: HttpRequest, topic: str) -> bool:  # noqa: ARG001
    return True


def signed(event_id: int, *topics: str) -> str:
    """The id an anonymous stream for `topics` sends for `event_id`."""
    return messages.EventIds(messages.Owner(()), list(topics)).sign(event_id)


def terminate_listener() -> None:
    """Ends every hub's LISTEN session on the test database, as a database restart would."""
    # current_database(): other projects on the same server keep their listeners.
    query = (
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE %s AND datname = current_database()"
    )
    with connection.cursor() as cursor:
        cursor.execute(query, ["LISTEN %"])


# An id no event has: a NOTIFY that names it comes from outside broadcast().
FORGED_ID = 2**62


def notify(payload: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_notify(%s, %s)", [messages.CHANNEL, payload])


def stops_the_hub[T: TransactionTestCase](test: Callable[[T], Awaitable[None]]) -> Callable[[T], Awaitable[None]]:
    """Close the hub's connections in the test's own event loop.

    Django's test cases run no asyncTearDown, and the loop ends with the test.
    """

    @functools.wraps(test)
    async def wrapper(self: T) -> None:
        try:
            await test(self)
        finally:
            await hub_module.get_hub().stop()

    return wrapper


def content_of(response: HttpResponseBase) -> Stream:
    assert isinstance(response, StreamingHttpResponse)
    content = response.streaming_content
    # An async view streams an async iterator, which an ASGI server sends without buffering.
    assert isinstance(content, AsyncIterable)
    return aiter(content)


class StreamTestCase(TransactionTestCase):
    async def open_stream(
        self, *topics: str, client: AsyncClient | None = None, headers: dict[str, str] | None = None
    ) -> Stream:
        """A stream whose subscription is in place, after its opening comment."""
        query = "&".join(f"topic={topic}" for topic in topics)
        response = await (client or AsyncClient()).get(f"/events/?{query}", headers=headers or {})
        self.assertEqual(response["Content-Type"], "text/event-stream")
        stream = content_of(response)
        self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.CONNECTED.encode())
        return stream

    async def next_message(self, stream: Stream, timeout: float = 5) -> str:
        """The next message, skipping comments such as keepalives."""
        while True:
            chunk = (await asyncio.wait_for(anext(stream), timeout)).decode()
            if not chunk.startswith(":"):
                return chunk


class StreamTest(StreamTestCase):
    async def test_requires_a_topic(self):
        response = await AsyncClient().get("/events/")
        self.assertEqual(response.status_code, 400)

    async def test_refuses_a_topic_the_user_may_not_read(self):
        response = await AsyncClient().get("/events/?topic=public:news&topic=room:1")
        self.assertEqual(response.status_code, 403)

    async def test_works_with_atomic_requests(self):
        # Django refuses an async view inside ATOMIC_REQUESTS unless the view opts out.
        with mock.patch.dict(connections.settings[DEFAULT_DB_ALIAS], ATOMIC_REQUESTS=True):
            response = await AsyncClient().get("/events/")
        self.assertEqual(response.status_code, 400)

    @override_settings(PG_SSE={"CAN_SUBSCRIBE": "tests.test_stream.allow_every_topic"})
    async def test_refuses_reserved_topics_whatever_can_subscribe_allows(self):
        for topic in ("_close", "_close:user:1", "_presence"):
            response = await AsyncClient().get(f"/events/?topic=public:news&topic={topic}")
            self.assertEqual(response.status_code, 403, topic)

    @stops_the_hub
    async def test_returns_its_connection_after_the_response_middleware(self):
        # Middleware such as SESSION_SAVE_EVERY_REQUEST queries after the view returns.
        with mock.patch.object(signals, "close_connection") as close_connection:
            response = await AsyncClient().get("/events/?topic=public:news")
            close_connection.assert_not_called()
            await asyncio.wait_for(anext(content_of(response)), 5)
        # The view closes the request thread's connection before the stream starts.
        close_connection.assert_called_with()

    @stops_the_hub
    async def test_an_unexpected_error_restarts_the_listener(self):
        stream = await self.open_stream("public:news")
        hub = hub_module.get_hub()
        receive = hub._receive  # pyright: ignore[reportPrivateUsage]
        failed: list[str] = []

        async def fail_once(payloads: list[str]) -> None:
            if not failed:
                failed.extend(payloads)
                raise RuntimeError(payloads)
            await receive(payloads)

        with mock.patch.object(hub, "_receive", fail_once), self.assertLogs(hub_module.logger, "ERROR"):
            await sync_to_async(publish.broadcast)("public:news", "<p>first</p>")
            self.assertIn("<p>first</p>", await self.next_message(stream, timeout=10))

    # One pooled connection, held below, so live delivery through the pool would block.
    @override_settings(PG_SSE={"STREAM_CONNECTIONS": 1})
    @stops_the_hub
    async def test_a_running_replay_leaves_live_delivery_free(self):
        stream = await self.open_stream("public:news")
        async with hub_module.get_hub().streams.connection():
            await sync_to_async(publish.broadcast)("public:news", "<p>live</p>")
            self.assertIn("<p>live</p>", await self.next_message(stream))

    @override_settings(PG_SSE={"STREAM_CONNECTIONS": 2})
    async def test_opening_streams_query_in_parallel(self):
        hub = hub_module.Hub()
        try:
            async with hub.streams.connection():
                self.assertFalse(await asyncio.wait_for(hub.any_since(["public:news"], 0), 2))
        finally:
            await hub.stop()

    async def test_a_stopped_hub_opens_no_new_pool(self):
        # A stream may outlive Hub.stop(), as at loop shutdown.
        hub = hub_module.Hub()
        await hub.streams.fetch(sql.SQL("SELECT 1"), [])
        await hub.stop()
        with self.assertRaises(RuntimeError):
            await hub.streams.fetch(sql.SQL("SELECT 1"), [])
        self.assertIsNone(hub.streams.pool)

    async def test_stop_runs_its_callbacks_before_the_pool_closes(self):
        # The presence tracker writes through the pool until it stops.
        hub = hub_module.Hub()
        await hub.streams.fetch(sql.SQL("SELECT 1"), [])
        pool_open: list[bool] = []

        async def callback() -> None:
            pool_open.append(not hub.streams.closed)

        hub.on_stop.append(callback)
        await hub.stop()
        self.assertEqual(pool_open, [True])

    async def test_a_failing_callback_still_closes_the_connections(self):
        hub = hub_module.Hub()
        await hub.streams.fetch(sql.SQL("SELECT 1"), [])

        async def fail() -> None:
            raise RuntimeError

        hub.on_stop.append(fail)
        with self.assertRaises(RuntimeError):
            await hub.stop()
        self.assertTrue(hub.streams.closed)
        self.assertIsNone(hub.streams.pool)

    async def test_get_hub_can_look_without_starting_one(self):
        # A stream that closes after its hub stopped must not start another.
        self.assertIsNone(hub_module.get_hub(create=False))
        hub = hub_module.get_hub()
        try:
            self.assertIs(hub_module.get_hub(create=False), hub)
        finally:
            await hub.stop()
        self.assertIsNone(hub_module.get_hub(create=False))

    async def test_a_closed_connection_reconnects(self):
        # A stream may outlive Hub.stop(), as at loop shutdown.
        hub_connection = hub_module._Connection()  # pyright: ignore[reportPrivateUsage]
        try:
            await hub_connection.fetch(sql.SQL("SELECT 1"), [])
            assert hub_connection.connection is not None
            await hub_connection.connection.close()
            self.assertEqual(await hub_connection.fetch(sql.SQL("SELECT 1"), []), [(1,)])
        finally:
            await hub_connection.close()

    @stops_the_hub
    async def test_an_id_beyond_the_newest_event_resumes_nothing(self):
        # After a database restore or a sequence reset, new events have smaller ids than the browser's.
        stream = await self.open_stream("public:news", headers={"Last-Event-ID": signed(10**9, "public:news")})
        await sync_to_async(publish.broadcast)("public:news", "<p>new</p>")
        self.assertIn("<p>new</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_an_id_this_worker_has_not_seen_yet_still_resumes(self):
        # Another worker sent the id; this worker's listener has not read its NOTIFY yet.
        await self.open_stream("public:news")
        seen = await StreamEvent.objects.acreate(topic="public:news", data="seen")
        await StreamEvent.objects.acreate(topic="public:news", data="missed")
        hub = hub_module.get_hub()
        assert hub.last_id is not None and hub.last_id < seen.id
        stream = await self.open_stream("public:news", headers={"Last-Event-ID": signed(seen.id, "public:news")})
        self.assertIn("missed", await self.next_message(stream))

    async def test_catch_up_follows_a_table_that_restarted_its_ids(self):
        # A database restore leaves last_id beyond every stored id; _deliver would drop new events.
        event = await StreamEvent.objects.acreate(topic="public:news", data="after restore")
        hub = hub_module.Hub()
        hub.last_id = event.id + 1000
        try:
            await hub._catch_up()  # pyright: ignore[reportPrivateUsage]
        finally:
            await hub.stop()
        self.assertEqual(hub.last_id, event.id)

    @stops_the_hub
    async def test_a_request_without_a_session_opens_a_stream(self):
        # Token authentication sets request.user but no session.
        user = await User.objects.acreate_user("ada")
        request = RequestFactory().get(f"/events/?topic=user:{user.pk}")
        request.user = user
        response = await views.stream(request)
        self.assertEqual(await asyncio.wait_for(anext(content_of(response)), 5), messages.CONNECTED.encode())

    @stops_the_hub
    async def test_the_stream_asks_nginx_not_to_buffer(self):
        # nginx buffers proxied responses unless the response opts out.
        response = await AsyncClient().get("/events/?topic=public:news")
        self.assertEqual(response["X-Accel-Buffering"], "no")
        assert isinstance(response, StreamingHttpResponse)
        content = response.streaming_content
        assert isinstance(content, AsyncGenerator)
        await content.aclose()

    @stops_the_hub
    async def test_a_stream_waiting_for_the_listener_sends_keepalives(self):
        # A proxy idle timeout would otherwise cut a stream that opens while the hub reconnects.
        hub = hub_module.get_hub()
        with mock.patch.object(hub, "_listen", mock.AsyncMock()), mock.patch.object(views, "KEEPALIVE_SECONDS", 0.1):
            stream = content_of(await AsyncClient().get("/events/?topic=public:news"))
            self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.KEEPALIVE.encode())

    @stops_the_hub
    async def test_a_slow_replay_sends_keepalives(self):
        # A busy pool can hold the opening queries longer than a proxy's idle timeout.
        seen = await StreamEvent.objects.acreate(topic="public:news", data="seen")
        await StreamEvent.objects.acreate(topic="public:news", data="missed")
        hub = hub_module.get_hub()
        replay = hub.replay

        async def slow_replay(topics: list[str], after_id: int) -> list[messages.Message]:
            await asyncio.sleep(0.5)
            return await replay(topics, after_id)

        headers = {"Last-Event-ID": signed(seen.id, "public:news")}
        with mock.patch.object(hub, "replay", slow_replay), mock.patch.object(views, "KEEPALIVE_SECONDS", 0.1):
            stream = await self.open_stream("public:news", headers=headers)
            self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.KEEPALIVE.encode())
            self.assertIn("missed", await self.next_message(stream))

    @stops_the_hub
    async def test_a_stream_whose_listener_never_starts_ends(self):
        # The browser then reconnects, perhaps to a worker whose listener runs.
        hub = hub_module.get_hub()
        patches = (
            mock.patch.object(hub, "_listen", mock.AsyncMock()),
            mock.patch.object(views, "KEEPALIVE_SECONDS", 0.1),
            mock.patch.object(views, "READY_TIMEOUT", 0.3, create=False),
        )
        with patches[0], patches[1], patches[2]:
            stream = content_of(await AsyncClient().get("/events/?topic=public:news"))
            with self.assertRaises(StopAsyncIteration):
                while True:
                    await asyncio.wait_for(anext(stream), 5)

    @stops_the_hub
    async def test_a_quiet_stream_sends_keepalives(self):
        # Proxies close a connection that stays silent; a comment line keeps it open.
        with mock.patch.object(views, "KEEPALIVE_SECONDS", 0.1):
            stream = await self.open_stream("public:news")
            self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.KEEPALIVE.encode())

    @stops_the_hub
    async def test_delivers_a_broadcast_to_the_open_stream(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:news", "<p>hi</p>")
        message = await self.next_message(stream)
        event = await StreamEvent.objects.aget()
        self.assertEqual(message, f"id: {signed(event.id, 'public:news')}\ndata: <p>hi</p>\n\n")

    @stops_the_hub
    async def test_delivers_json_as_a_named_event(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast_json)("public:news", {"online": 12}, event="presence")
        message = await self.next_message(stream)
        event = await StreamEvent.objects.aget()
        expected = f'id: {signed(event.id, "public:news")}\nevent: presence\ndata: {{"online":12}}\n\n'
        self.assertEqual(message, expected)

    @stops_the_hub
    async def test_other_topics_do_not_reach_the_stream(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:other", "<p>no</p>")
        await sync_to_async(publish.broadcast)("public:news", "<p>yes</p>")
        self.assertIn("<p>yes</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_last_event_id_replays_missed_events(self):
        seen = await StreamEvent.objects.acreate(topic="public:news", data="seen")
        missed = await StreamEvent.objects.acreate(topic="public:news", data="missed")
        await StreamEvent.objects.acreate(topic="public:other", data="elsewhere")
        stream = await self.open_stream("public:news", headers={"Last-Event-ID": signed(seen.id, "public:news")})
        self.assertEqual(await self.next_message(stream), f"id: {signed(missed.id, 'public:news')}\ndata: missed\n\n")

    @stops_the_hub
    async def test_replay_skips_snapshots_that_a_newer_one_replaces(self):
        # An online list replaces the previous one; only the newest is worth replaying.
        seen = await StreamEvent.objects.acreate(topic="public:news", data="seen")
        await StreamEvent.objects.acreate(topic="public:news", data="old list", latest="online")
        message = await StreamEvent.objects.acreate(topic="public:news", data="message")
        await StreamEvent.objects.acreate(topic="public:other", data="other topic", latest="online")
        new_list = await StreamEvent.objects.acreate(topic="public:news", data="new list", latest="online")
        count = await StreamEvent.objects.acreate(topic="public:news", data="count", latest="count")
        stream = await self.open_stream("public:news", headers={"Last-Event-ID": signed(seen.id, "public:news")})
        replayed = [await self.next_message(stream) for _ in range(3)]
        expected = [(message.id, "message"), (new_list.id, "new list"), (count.id, "count")]
        self.assertEqual(replayed, [f"id: {signed(i, 'public:news')}\ndata: {d}\n\n" for i, d in expected])

    @stops_the_hub
    async def test_live_delivery_sends_every_snapshot(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:news", "first list", latest="online")
        await sync_to_async(publish.broadcast)("public:news", "second list", latest="online")
        self.assertIn("first list", await self.next_message(stream))
        self.assertIn("second list", await self.next_message(stream))

    @stops_the_hub
    async def test_an_unsigned_last_event_id_replays_nothing(self):
        # Otherwise a client could pick any id and read the whole retention of a topic.
        seen = await StreamEvent.objects.acreate(topic="public:news", data="seen")
        await StreamEvent.objects.acreate(topic="public:news", data="missed")
        stream = await self.open_stream("public:news", headers={"Last-Event-ID": str(seen.id)})
        await sync_to_async(publish.broadcast)("public:news", "<p>live</p>")
        self.assertIn("<p>live</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_the_id_a_stream_sends_resumes_it(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:news", "<p>seen</p>")
        received = await self.next_message(stream)
        last_event_id = received.split("\n", 1)[0].removeprefix("id: ")
        await StreamEvent.objects.acreate(topic="public:news", data="missed")
        resumed = await self.open_stream("public:news", headers={"Last-Event-ID": last_event_id})
        self.assertIn("missed", await self.next_message(resumed))

    @stops_the_hub
    async def test_a_foreign_notify_leaves_the_listener_running(self):
        stream = await self.open_stream("public:news")
        with self.assertLogs(hub_module.logger, "WARNING"):
            await sync_to_async(notify)("ping")
            await sync_to_async(publish.broadcast)("public:news", "<p>still</p>")
            self.assertIn("<p>still</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_a_long_replay_ends_the_stream_after_one_batch(self):
        # htmx reconnects with the last id it got, so the next batch follows.
        first = await StreamEvent.objects.acreate(topic="public:news", data="one")
        await StreamEvent.objects.acreate(topic="public:news", data="two")
        await StreamEvent.objects.acreate(topic="public:news", data="three")
        with mock.patch.object(hub_module, "REPLAY_BATCH", 1):
            headers = {"Last-Event-ID": signed(first.id, "public:news")}
            stream = await self.open_stream("public:news", headers=headers)
            self.assertIn("two", await self.next_message(stream))
            with self.assertRaises(StopAsyncIteration):
                await self.next_message(stream)

    @stops_the_hub
    async def test_an_unread_stream_leaves_no_subscription(self):
        request = RequestFactory().get("/events/?topic=public:news")
        request.user = AnonymousUser()
        request.session = SessionStore()
        response = await views.stream(request)
        assert isinstance(response, StreamingHttpResponse)
        content: Any = response.streaming_content
        await content.aclose()
        self.assertEqual(hub_module.get_hub().queues, {})

    @stops_the_hub
    async def test_events_sent_while_the_listener_reconnects_arrive(self):
        stream = await self.open_stream("public:news")
        with self.assertLogs(hub_module.logger, "WARNING"):
            await sync_to_async(terminate_listener)()
            await sync_to_async(publish.broadcast)("public:news", "<p>during</p>")
            self.assertIn("<p>during</p>", await self.next_message(stream, timeout=10))

    @stops_the_hub
    async def test_the_catch_up_skips_snapshots_that_a_newer_one_replaces(self):
        # Stored without NOTIFY while the listener is gone, so only the catch-up delivers them.
        stream = await self.open_stream("public:news")
        with self.assertLogs(hub_module.logger, "WARNING"):
            await sync_to_async(terminate_listener)()
            await StreamEvent.objects.acreate(topic="public:news", data="old list", latest="online")
            await StreamEvent.objects.acreate(topic="public:news", data="new list", latest="online")
            self.assertIn("new list", await self.next_message(stream, timeout=10))
        await sync_to_async(publish.broadcast)("public:news", "after")
        self.assertIn("after", await self.next_message(stream))


class CloseStreamsTest(StreamTestCase):
    async def logged_in_client(self, user: User) -> AsyncClient:
        client = AsyncClient()
        await client.aforce_login(user)
        return client

    async def assert_closed(self, stream: Stream) -> None:
        self.assertEqual(await self.next_message(stream), messages.CLOSED)
        with self.assertRaises(StopAsyncIteration):
            await self.next_message(stream)

    @stops_the_hub
    async def test_logout_closes_the_streams_of_that_session_only(self):
        user = await User.objects.acreate_user("ada")
        first, second = await self.logged_in_client(user), await self.logged_in_client(user)
        first_stream = await self.open_stream(f"user:{user.pk}", client=first)
        second_stream = await self.open_stream(f"user:{user.pk}", client=second)
        await first.alogout()
        await self.assert_closed(first_stream)
        await sync_to_async(publish.broadcast)(f"user:{user.pk}", "<p>still here</p>")
        self.assertIn("still here", await self.next_message(second_stream))

    async def signed_for(self, client: AsyncClient, user: User, event_id: int, *topics: str) -> str:
        session = await client.asession()
        owner = messages.Owner.of(user=str(user.pk), session=session.session_key)
        return messages.EventIds(owner, list(topics)).sign(event_id)

    @stops_the_hub
    async def test_an_id_signed_for_another_owner_replays_nothing(self):
        user = await User.objects.acreate_user("ada")
        client = await self.logged_in_client(user)
        seen = await StreamEvent.objects.acreate(topic="public:news", data="seen")
        await StreamEvent.objects.acreate(topic="public:news", data="missed")
        headers = {"Last-Event-ID": signed(seen.id, "public:news")}
        stream = await self.open_stream("public:news", client=client, headers=headers)
        await sync_to_async(publish.broadcast)("public:news", "<p>live</p>")
        self.assertIn("<p>live</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_a_close_after_the_id_prevents_its_replay(self):
        # The user lost access and got it back; the events in between stay unread.
        user = await User.objects.acreate_user("ada")
        client = await self.logged_in_client(user)
        topic = f"user:{user.pk}"
        seen = await StreamEvent.objects.acreate(topic=topic, data="seen")
        await sync_to_async(publish.close_streams)(user=user)
        await StreamEvent.objects.acreate(topic=topic, data="while revoked")
        headers = {"Last-Event-ID": await self.signed_for(client, user, seen.id, topic)}
        stream = await self.open_stream(topic, client=client, headers=headers)
        await sync_to_async(publish.broadcast)(topic, "<p>live</p>")
        self.assertIn("<p>live</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_an_authenticated_stream_resumes_its_own_id(self):
        user = await User.objects.acreate_user("ada")
        client = await self.logged_in_client(user)
        seen = await StreamEvent.objects.acreate(topic="public:news", data="seen")
        await StreamEvent.objects.acreate(topic="public:news", data="missed")
        headers = {"Last-Event-ID": await self.signed_for(client, user, seen.id, "public:news")}
        stream = await self.open_stream("public:news", client=client, headers=headers)
        self.assertIn("missed", await self.next_message(stream))

    @stops_the_hub
    async def test_closing_by_user_closes_every_session(self):
        user = await User.objects.acreate_user("ada")
        streams = [
            await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user)) for _ in range(2)
        ]
        await sync_to_async(publish.close_streams)(user=user)
        for stream in streams:
            await self.assert_closed(stream)

    @stops_the_hub
    async def test_a_rolled_back_close_leaves_the_stream_open(self):
        user = await User.objects.acreate_user("ada")
        stream = await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user))

        def close_then_roll_back() -> None:
            with transaction.atomic():
                publish.close_streams(user=user)
                transaction.set_rollback(True)

        await sync_to_async(close_then_roll_back)()
        await sync_to_async(publish.broadcast)(f"user:{user.pk}", "<p>open</p>")
        self.assertIn("open", await self.next_message(stream))

    @stops_the_hub
    async def test_a_close_sent_while_the_listener_reconnects_arrives(self):
        user = await User.objects.acreate_user("ada")
        stream = await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user))
        with self.assertLogs(hub_module.logger, "WARNING"):
            await sync_to_async(terminate_listener)()
            await sync_to_async(publish.close_streams)(user=user)
            await self.assert_closed(stream)

    @stops_the_hub
    async def test_a_close_before_the_stream_starts_reaches_it(self):
        # The view authorises; the stream subscribes later, when its first chunk is read.
        user = await User.objects.acreate_user("ada")
        client = await self.logged_in_client(user)
        stream = content_of(await client.get(f"/events/?topic=user:{user.pk}"))
        await sync_to_async(publish.close_streams)(user=user)
        self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.CONNECTED.encode())
        await self.assert_closed(stream)

    async def test_a_closed_stream_stays_closed_when_its_queue_overflows(self):
        hub = hub_module.Hub()
        owner = messages.Owner.of(user="7", session=None)
        with mock.patch.object(hub_module, "QUEUE_SIZE", 2):
            queue = hub.subscribe(["public:news"], owner)
        try:
            (close_topic,) = owner.close_topics
            hub._deliver(messages.Message(1, close_topic, "", ""))  # pyright: ignore[reportPrivateUsage]
            for event_id in range(2, 6):
                hub._deliver(messages.Message(event_id, "public:news", "", "<p>x</p>"))  # pyright: ignore[reportPrivateUsage]
            self.assertEqual([queue.get_nowait() for _ in range(queue.qsize())], [messages.Signal.REVOKED])
        finally:
            await hub.stop()

    @stops_the_hub
    async def test_a_notify_without_a_stored_close_ends_no_stream(self):
        # Every role that can connect to the database can NOTIFY the channel.
        user = await User.objects.acreate_user("ada")
        stream = await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user))
        (close_topic,) = messages.Owner.of(user=str(user.pk), session=None).close_topics
        hub = hub_module.get_hub()
        await sync_to_async(notify)(messages.notify_payload((hub.last_id or 0) + 1, close_topic))
        await sync_to_async(publish.broadcast)(f"user:{user.pk}", "<p>still here</p>")
        self.assertIn("still here", await self.next_message(stream))

    @stops_the_hub
    async def test_a_notify_with_an_unknown_id_stops_no_delivery(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(notify)(messages.notify_payload(FORGED_ID, "public:news"))
        await sync_to_async(publish.broadcast)("public:news", "<p>live</p>")
        self.assertIn("<p>live</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_a_notify_with_an_unknown_id_skips_no_check_for_missed_closes(self):
        user = await User.objects.acreate_user("ada")
        client = await self.logged_in_client(user)
        # Kept: the garbage collector would close a dropped stream at a random moment.
        _news = await self.open_stream("public:news")
        hub = hub_module.get_hub()
        await sync_to_async(notify)(messages.notify_payload(FORGED_ID, "public:other"))
        await sync_to_async(publish.broadcast)("public:other", "<p>unread</p>")
        unread = await StreamEvent.objects.alatest("id")
        # NOTIFYs arrive in order, so both have reached the hub once last_id counts the real one.
        async with asyncio.timeout(5):
            while (hub.last_id or 0) < unread.id:
                await asyncio.sleep(0.01)
        stream = content_of(await client.get(f"/events/?topic=user:{user.pk}"))
        await sync_to_async(publish.close_streams)(user=user)
        self.assertEqual(await asyncio.wait_for(anext(stream), 5), messages.CONNECTED.encode())
        await self.assert_closed(stream)

    async def test_another_owners_close_counts_without_loading(self):
        ada, bob = await User.objects.acreate_user("ada"), await User.objects.acreate_user("bob")
        await sync_to_async(publish.close_streams)(user=bob)
        close = await StreamEvent.objects.aget()
        hub = hub_module.Hub()
        try:
            with mock.patch.object(hub, "_listen", mock.AsyncMock()):
                queue = hub.subscribe(["public:news"], messages.Owner.of(user=str(ada.pk), session=None))
            await hub._receive([messages.notify_payload(close.id, close.topic)])  # pyright: ignore[reportPrivateUsage]
        finally:
            await hub.stop()
        self.assertTrue(queue.empty())
        self.assertEqual(hub.last_id, close.id)


class ReceiveTest(SimpleTestCase):
    async def test_pending_notifies_load_in_one_query(self):
        hub = hub_module.Hub()
        with mock.patch.object(hub, "_listen", mock.AsyncMock()):
            queue = hub.subscribe(["public:news"], messages.Owner(()))
        one, three = messages.Message(1, "public:news", "", "one"), messages.Message(3, "public:news", "", "three")
        # The unread event 2 comes without its data.
        rows = [(1, "public:news", "", "one"), (2, "public:other", "", None), (3, "public:news", "", "three")]
        with mock.patch.object(hub.live, "fetch", mock.AsyncMock(return_value=rows)) as fetch:
            # The unread id 2 lies between the two; last_id must not pass 3 before it is delivered.
            await hub._receive(["1:public:news", "2:public:other", "3:public:news"])  # pyright: ignore[reportPrivateUsage]
        fetch.assert_awaited_once_with(mock.ANY, [["public:news"], [1, 2, 3]])
        self.assertEqual([queue.get_nowait(), queue.get_nowait()], [one, three])
        self.assertEqual(hub.last_id, 3)

    async def test_a_stream_subscribed_during_the_query_gets_its_events(self):
        hub = hub_module.Hub()
        owner = messages.Owner.of(user="7", session=None)
        close_topic = owner.close_topics[0]
        with mock.patch.object(hub, "_listen", mock.AsyncMock()):
            hub.subscribe(["public:news"], messages.Owner(()))
        late: list[hub_module.Queue] = []
        rows = {1: (1, "public:news", "", "one"), 2: (2, close_topic, "", ""), 3: (3, "public:other", "", "three")}

        async def fetch(query: object, params: list[list[Any]]) -> list[tuple[object, ...]]:  # noqa: ARG001
            if not late:
                with mock.patch.object(hub, "_listen", mock.AsyncMock()):
                    late.append(hub.subscribe(["public:other"], owner))
            if len(params) == 1:
                return [rows[event_id] for event_id in params[0]]
            read, ids = params
            return [(*rows[event_id][:3], rows[event_id][3] if rows[event_id][1] in read else None) for event_id in ids]

        with mock.patch.object(hub.live, "fetch", fetch):
            payloads = ["1:public:news", f"2:{close_topic}", "3:public:other"]
            await hub._receive(payloads)  # pyright: ignore[reportPrivateUsage]
        # The close ends the late queue, so event 3 never reaches it.
        self.assertEqual([late[0].get_nowait()], [messages.Signal.REVOKED])
        self.assertEqual(hub.last_id, 3)

    async def test_a_missing_row_counts_for_nothing(self):
        # Another role sent the NOTIFY, or prune_stream_events deleted the event before the query.
        hub = hub_module.Hub()
        with mock.patch.object(hub, "_listen", mock.AsyncMock()):
            queue = hub.subscribe(["public:news"], messages.Owner.of(user="7", session=None))
        close_topic = messages.Owner.of(user="7", session=None).close_topics[0]
        with mock.patch.object(hub.live, "fetch", mock.AsyncMock(return_value=[])) as fetch:
            await hub._receive(["1:public:news", f"2:{close_topic}"])  # pyright: ignore[reportPrivateUsage]
        fetch.assert_awaited_once()
        self.assertTrue(queue.empty())
        self.assertIsNone(hub.last_id)

    async def test_a_stored_close_loads_in_the_same_query(self):
        hub = hub_module.Hub()
        owner = messages.Owner.of(user="7", session=None)
        with mock.patch.object(hub, "_listen", mock.AsyncMock()):
            queue = hub.subscribe(["public:news"], owner)
        rows = [(5, owner.close_topics[0], "", "")]
        with mock.patch.object(hub.live, "fetch", mock.AsyncMock(return_value=rows)) as fetch:
            await hub._receive([messages.notify_payload(5, owner.close_topics[0])])  # pyright: ignore[reportPrivateUsage]
        fetch.assert_awaited_once()
        self.assertIs(queue.get_nowait(), messages.Signal.REVOKED)


def backend_pid_of_listener() -> int:
    # Read before the first health check, which replaces LISTEN as the backend's last query.
    query = "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'LISTEN %%'"
    with connection.cursor() as cursor:
        cursor.execute(query)
        ((pid,),) = cursor.fetchall()
        return pid


def is_local_postgres(pid: int) -> bool:
    # The pid comes from the server; only a local server in the same pid namespace owns it here.
    result = subprocess.run(["ps", "-p", str(pid), "-o", "comm="], capture_output=True, text=True, check=False)
    return "postgres" in result.stdout


def terminate_backend(pid: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_terminate_backend(%s)", [pid])


class HubRobustnessTest(StreamTestCase):
    @stops_the_hub
    async def test_a_silent_listen_connection_is_replaced(self):
        # SIGSTOP keeps the TCP connection open but silent, as a half-open connection after a failover.
        with mock.patch.multiple(hub_module, HEALTH_SECONDS=1, HEALTH_TIMEOUT=0.5):
            stream = await self.open_stream("public:news")
            pid = await sync_to_async(backend_pid_of_listener)()
            if not await asyncio.to_thread(is_local_postgres, pid):
                self.skipTest("The PostgreSQL server is not a local process.")
            try:
                os.kill(pid, signal.SIGSTOP)
            except (PermissionError, ProcessLookupError):
                self.skipTest("The PostgreSQL backend cannot be stopped from here.")
            try:
                with self.assertLogs(hub_module.logger, "WARNING"):
                    await sync_to_async(publish.broadcast)("public:news", "<p>after</p>")
                    self.assertIn("<p>after</p>", await self.next_message(stream, timeout=10))
            finally:
                os.kill(pid, signal.SIGCONT)

    async def test_catch_up_advances_past_topics_nobody_reads(self):
        # A stream that subscribes during the catch-up then sees the ids it missed and checks for closes.
        await StreamEvent.objects.acreate(topic="public:news", data="seen")
        unread = await StreamEvent.objects.acreate(topic="_close:user:x", data="")
        hub = hub_module.Hub()
        hub.last_id = unread.id - 1
        try:
            await hub._catch_up()  # pyright: ignore[reportPrivateUsage]
        finally:
            await hub.stop()
        self.assertEqual(hub.last_id, unread.id)

    @stops_the_hub
    async def test_a_starting_listener_delivers_what_commits_after_its_start(self):
        await StreamEvent.objects.acreate(topic="public:news", data="<p>before start</p>")
        hub = hub_module.get_hub()
        fetch = hub.live.fetch
        reads: list[int] = []

        async def commit_an_event_first(query: Any, params: list[Any]) -> list[tuple[Any, ...]]:
            # Each read of the newest id races with a broadcast that commits just before it.
            if "max(id)" in str(query):
                reads.append(len(reads))
                await sync_to_async(publish.broadcast)("public:news", f"<p>read {len(reads)}</p>")
            return await fetch(query, params)

        with mock.patch.object(hub.live, "fetch", commit_an_event_first):
            stream = await self.open_stream("public:news")
            # The first read sets the start; what commits after it, before LISTEN or not, arrives.
            self.assertIn("<p>read 2</p>", await self.next_message(stream))

    @override_settings(PG_SSE={"STREAM_CONNECTIONS": 3})
    async def test_a_pool_whose_connections_all_ended_recovers(self):
        hub = hub_module.Hub()
        try:
            async with hub.streams.connection() as a, hub.streams.connection() as b, hub.streams.connection() as c:
                pids = [conn.info.backend_pid for conn in (a, b, c)]
            for pid in pids:
                await sync_to_async(terminate_backend)(pid)
            self.assertEqual(await hub.streams.fetch(sql.SQL("SELECT 1"), []), [(1,)])
        finally:
            await hub.stop()

    async def test_a_failed_connect_is_not_retried(self):
        hub_connection = hub_module._Connection()  # pyright: ignore[reportPrivateUsage]
        refused = psycopg.OperationalError("refused")
        with mock.patch.object(hub_module, "_connect", side_effect=refused) as connect:
            with self.assertRaises(psycopg.OperationalError):
                await hub_connection.fetch(sql.SQL("SELECT 1"), [])
        connect.assert_called_once()

    @contextlib.asynccontextmanager
    async def stopped_backend(self, pid: int) -> AsyncGenerator[None, None]:
        """SIGSTOP keeps the backend's TCP connection open but silent, as after a failover."""
        if not await asyncio.to_thread(is_local_postgres, pid):
            self.skipTest("The PostgreSQL server is not a local process.")
        try:
            os.kill(pid, signal.SIGSTOP)
        except (PermissionError, ProcessLookupError):
            self.skipTest("The PostgreSQL backend cannot be stopped from here.")
        try:
            yield
        finally:
            os.kill(pid, signal.SIGCONT)

    @override_settings(PG_SSE={"QUERY_TIMEOUT": 0.5})
    async def test_a_silent_live_connection_gives_up_and_reconnects(self):
        hub_connection = hub_module._Connection()  # pyright: ignore[reportPrivateUsage]
        try:
            ((pid,),) = await hub_connection.fetch(sql.SQL("SELECT pg_backend_pid()"), [])
            async with self.stopped_backend(pid):
                rows = await asyncio.wait_for(hub_connection.fetch(sql.SQL("SELECT 1"), []), 15)
            self.assertEqual(rows, [(1,)])
        finally:
            await hub_connection.close()

    @override_settings(PG_SSE={"STREAM_CONNECTIONS": 1, "QUERY_TIMEOUT": 0.5})
    async def test_a_silent_pooled_connection_gives_up_and_reconnects(self):
        hub = hub_module.Hub()
        try:
            ((pid,),) = await hub.streams.fetch(sql.SQL("SELECT pg_backend_pid()"), [])
            async with self.stopped_backend(pid):
                rows = await asyncio.wait_for(hub.streams.fetch(sql.SQL("SELECT 1"), []), 15)
            self.assertEqual(rows, [(1,)])
        finally:
            await hub.stop()

    @override_settings(PG_SSE={"STREAM_CONNECTIONS": 1, "QUERY_TIMEOUT": 0.5})
    async def test_a_hanging_pool_check_gives_up(self):
        # Another half-open idle connection would hold pool.check() before the retry.
        hub = hub_module.Hub()
        fetch = hub_module._fetch  # pyright: ignore[reportPrivateUsage]
        failed: list[bool] = []

        async def lose_the_connection_once(conn: Any, query: Any, params: list[Any]) -> list[tuple[Any, ...]]:
            if not failed:
                failed.append(True)
                await conn.close()
                raise psycopg.OperationalError("server closed the connection unexpectedly")
            return await fetch(conn, query, params)

        try:
            await hub.streams.fetch(sql.SQL("SELECT 1"), [])
            pool = await hub.streams._open()  # pyright: ignore[reportPrivateUsage]

            async def hang() -> None:
                await asyncio.sleep(60)

            hang_check = mock.patch.object(pool, "check", hang)
            with hang_check, mock.patch.object(hub_module, "_fetch", lose_the_connection_once):
                with self.assertRaises(psycopg.OperationalError):
                    await asyncio.wait_for(hub.streams.fetch(sql.SQL("SELECT 1"), []), 10)
        finally:
            await hub.stop()

    @override_settings(PG_SSE={"QUERY_TIMEOUT": 0.2})
    async def test_a_slow_query_on_a_healthy_server_is_not_retried(self):
        hub_connection = hub_module._Connection()  # pyright: ignore[reportPrivateUsage]
        try:
            with self.assertRaises(psycopg.OperationalError):
                await asyncio.wait_for(hub_connection.fetch(sql.SQL("SELECT pg_sleep(2)"), []), 10)
            # The cancelled query leaves the connection usable; a retry would have slept again.
            self.assertEqual(await hub_connection.fetch(sql.SQL("SELECT 1"), []), [(1,)])
        finally:
            await hub_connection.close()

    def test_connections_give_up_connecting_after_ten_seconds(self):
        self.assertEqual(hub_module.connect_params()["connect_timeout"], 10)
        options = {**connection.settings_dict["OPTIONS"], "connect_timeout": 3}
        with mock.patch.dict(connection.settings_dict, OPTIONS=options):
            self.assertEqual(hub_module.connect_params()["connect_timeout"], 3)

    def test_libpq_connect_timeouts_from_the_environment_or_a_service_apply(self):
        # A default kwarg would override PGCONNECT_TIMEOUT and a service file's connect_timeout.
        with mock.patch.dict(os.environ, PGCONNECT_TIMEOUT="30"):
            self.assertNotIn("connect_timeout", hub_module.connect_params())
        with mock.patch.dict(os.environ, PGSERVICE="pg_sse"):
            self.assertNotIn("connect_timeout", hub_module.connect_params())
        options = {**connection.settings_dict["OPTIONS"], "service": "pg_sse"}
        with mock.patch.dict(connection.settings_dict, OPTIONS=options):
            self.assertNotIn("connect_timeout", hub_module.connect_params())

    def test_a_blocked_socket_gives_up_on_linux(self):
        # TCP_USER_TIMEOUT ends any send or receive the server stops acknowledging.
        self.assertEqual(hub_module.connect_params()["tcp_user_timeout"], 30000)

    async def test_a_terminated_connection_is_replaced(self):
        # A database restart ends every backend; the next query must not fail with it.
        hub = hub_module.Hub()
        try:
            for hub_connection in (hub.live, hub.streams):
                ((pid,),) = await hub_connection.fetch(sql.SQL("SELECT pg_backend_pid()"), [])
                await sync_to_async(terminate_backend)(pid)
                self.assertEqual(await hub_connection.fetch(sql.SQL("SELECT 1"), []), [(1,)])
        finally:
            await hub.stop()


class AsyncPublishTest(StreamTestCase):
    @stops_the_hub
    async def test_abroadcast_reaches_an_open_stream(self):
        stream = await self.open_stream("public:news")
        await publish.abroadcast("public:news", "<p>from async</p>")
        self.assertIn("<p>from async</p>", await self.next_message(stream))

    async def test_the_async_variants_pass_the_latest_key_on(self):
        await publish.abroadcast("public:news", "<ul></ul>", latest="online")
        await publish.abroadcast_json("public:news", {"online": 2}, event="count", latest="count")
        keys = [event.latest async for event in StreamEvent.objects.order_by("id")]
        self.assertEqual(keys, ["online", "count"])

    @stops_the_hub
    async def test_abroadcast_json_sends_a_named_event(self):
        stream = await self.open_stream("public:news")
        await publish.abroadcast_json("public:news", {"online": 12}, event="presence")
        self.assertIn('event: presence\ndata: {"online":12}', await self.next_message(stream))

    @stops_the_hub
    async def test_aclose_streams_ends_the_streams_of_a_user(self):
        user = await User.objects.acreate_user("ada")
        client = AsyncClient()
        await client.aforce_login(user)
        stream = await self.open_stream(f"user:{user.pk}", client=client)
        await publish.aclose_streams(user=user)
        self.assertEqual(await self.next_message(stream), messages.CLOSED)

    @stops_the_hub
    async def test_aclose_streams_ends_the_streams_of_a_session(self):
        user = await User.objects.acreate_user("ada")
        client = AsyncClient()
        await client.aforce_login(user)
        stream = await self.open_stream(f"user:{user.pk}", client=client)
        await publish.aclose_streams(session=await client.asession())
        self.assertEqual(await self.next_message(stream), messages.CLOSED)

    async def test_the_sync_functions_refuse_a_running_event_loop(self):
        # A robust on_commit callback would only log the error, and the event would be lost.
        user = await User.objects.acreate_user("ada")
        for call in (
            lambda: publish.broadcast("public:news", "x"),
            lambda: publish.broadcast_json("public:news", {}, event="stats"),
            lambda: publish.close_streams(user=user),
        ):
            with self.assertRaises(SynchronousOnlyOperation):
                call()

    async def test_closing_an_anonymous_user_is_refused(self):
        with self.assertRaises(ValueError):
            await publish.aclose_streams(user=AnonymousUser())  # pyright: ignore[reportArgumentType]

    async def test_the_async_variants_check_their_arguments(self):
        with self.assertRaises(ValueError):
            await publish.abroadcast("_close:user:1", "x")
        for event in ("closed", "hx:release", "a\nb"):
            with self.assertRaises(ValueError, msg=event):
                await publish.abroadcast("public:news", "x", event=event)
        with self.assertRaises(ValueError):
            await publish.abroadcast_json("public:news", {}, event="")
        with self.assertRaises(ValueError):
            await publish.aclose_streams()


class SignalsTest(StreamTestCase):
    def record(self) -> list[tuple[str, Any]]:
        received: list[tuple[str, Any]] = []
        for name, stream_signal in (("opened", signals.stream_opened), ("closed", signals.stream_closed)):

            def receiver(sender: Any, stream: Any, name: str = name, **kwargs: Any) -> None:  # noqa: ARG001
                received.append((name, stream))

            stream_signal.connect(receiver, weak=False)
            self.addCleanup(stream_signal.disconnect, receiver)
        return received

    async def logged_in_client(self, user: User) -> AsyncClient:
        client = AsyncClient()
        await client.aforce_login(user)
        return client

    @stops_the_hub
    async def test_an_opened_stream_names_its_owner_and_topics(self):
        received = self.record()
        user = await User.objects.acreate_user("ada")
        client = await self.logged_in_client(user)
        await self.open_stream(f"user:{user.pk}", "public:news", client=client)
        session_key = (await client.asession()).session_key
        assert session_key is not None
        ((name, stream),) = received
        self.assertEqual(name, "opened")
        self.assertRegex(stream.id, r"^[0-9a-f]{32}$")
        self.assertEqual(stream.user_pk, str(user.pk))
        self.assertEqual(stream.session, hashlib.sha256(session_key.encode()).hexdigest())
        self.assertEqual(stream.topics, (f"user:{user.pk}", "public:news"))

    @stops_the_hub
    async def test_an_opened_stream_carries_its_request(self):
        received = self.record()
        await self.open_stream("public:news")
        ((_, stream),) = received
        self.assertEqual(stream.request.path, "/events/")
        self.assertEqual(stream.request.GET.getlist("topic"), ["public:news"])

    @stops_the_hub
    async def test_an_anonymous_stream_has_no_owner(self):
        received = self.record()
        await self.open_stream("public:news")
        ((_, stream),) = received
        self.assertEqual((stream.user_pk, stream.session), (None, None))

    @stops_the_hub
    async def test_a_disconnect_sends_stream_closed(self):
        # Django cancels the response task when the client disconnects.
        received = self.record()
        stream = await self.open_stream("public:news")
        waiting = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.1)
        waiting.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await waiting
        self.assertEqual([name for name, _ in received], ["opened", "closed"])
        self.assertEqual(received[0][1], received[1][1])

    @stops_the_hub
    async def test_a_revoked_stream_sends_stream_closed(self):
        received = self.record()
        user = await User.objects.acreate_user("ada")
        stream = await self.open_stream(f"user:{user.pk}", client=await self.logged_in_client(user))
        await sync_to_async(publish.close_streams)(user=user)
        self.assertEqual(await self.next_message(stream), messages.CLOSED)
        with self.assertRaises(StopAsyncIteration):
            await self.next_message(stream)
        self.assertEqual([name for name, _ in received], ["opened", "closed"])

    @stops_the_hub
    async def test_a_failing_receiver_leaves_the_stream_running(self):
        def fail(sender: Any, **kwargs: Any) -> None:  # noqa: ARG001
            raise RuntimeError("receiver")

        signals.stream_opened.connect(fail, weak=False)
        self.addCleanup(signals.stream_opened.disconnect, fail)
        # Signal.asend_robust() logs the failure once, to django.dispatch.
        with self.assertLogs("django.dispatch", "ERROR"), self.assertNoLogs("django_pg_sse", "ERROR"):
            stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:news", "<p>still</p>")
        self.assertIn("<p>still</p>", await self.next_message(stream))

    @stops_the_hub
    async def test_a_disconnect_during_stream_opened_still_sends_stream_closed(self):
        received = self.record()
        started, release = threading.Event(), threading.Event()

        def slow(sender: Any, **kwargs: Any) -> None:  # noqa: ARG001
            started.set()
            release.wait(5)

        signals.stream_opened.connect(slow, weak=False)
        self.addCleanup(signals.stream_opened.disconnect, slow)
        stream = content_of(await AsyncClient().get("/events/?topic=public:news"))
        waiting = asyncio.ensure_future(anext(stream))
        self.assertTrue(await asyncio.to_thread(started.wait, 5))
        waiting.cancel()
        release.set()
        with contextlib.suppress(asyncio.CancelledError):
            await waiting
        self.assertIn("closed", [name for name, _ in received])

    @stops_the_hub
    async def test_a_receiver_leaves_no_connection_to_the_stream(self):
        # A sync receiver runs in the request's thread, whose connection the stream would keep.
        def query(sender: Any, **kwargs: Any) -> None:  # noqa: ARG001
            StreamEvent.objects.exists()

        signals.stream_opened.connect(query, weak=False)
        self.addCleanup(signals.stream_opened.disconnect, query)
        calls: list[str] = []

        def note(sender: Any, **kwargs: Any) -> None:  # noqa: ARG001
            calls.append("receiver")

        signals.stream_opened.connect(note, weak=False)
        self.addCleanup(signals.stream_opened.disconnect, note)
        with mock.patch.object(signals, "close_connection", side_effect=lambda: calls.append("close")):
            await self.open_stream("public:news")
        self.assertEqual(calls, ["close", "receiver", "close"])

    async def test_a_refused_or_unread_stream_sends_nothing(self):
        received = self.record()
        refused = await AsyncClient().get("/events/?topic=room:1")
        self.assertEqual(refused.status_code, 403)
        unread = await AsyncClient().get("/events/?topic=public:news")
        assert isinstance(unread, StreamingHttpResponse)
        content = unread.streaming_content
        assert isinstance(content, AsyncGenerator)
        await content.aclose()
        self.assertEqual(received, [])


def open_aliases() -> list[str]:
    """The aliases with an open connection in the calling thread."""
    return [alias for alias in ("default", "direct") if connections[alias].connection is not None]


@override_settings(PG_SSE={"DATABASE": "direct"})
class ReceiverThreadTest(TransactionTestCase):
    """Under ASGI a stream's sync thread lives as long as the stream, and so does its connection."""

    databases = {"default", "direct"}

    def setUp(self):
        super().setUp()
        # The tasks the receivers start, and the request's own sync thread.
        self.tasks: list[asyncio.Task[None]] = []
        self.request_thread = 0
        self.addCleanup(asyncio.run, self.close_shared_thread())

    async def in_shared_thread[T](self, function: Callable[[], T]) -> T:
        """Run `function` in the thread where asend() runs the receivers' sync code."""
        loop = asyncio.get_running_loop()
        return await loop.create_task(sync_to_async(function)(), context=signals.outside_request_thread())

    async def close_shared_thread(self) -> None:
        await self.in_shared_thread(connections.close_all)

    def send_in_a_request(self, receiver: Callable[..., Any]) -> tuple[list[str], list[str]]:
        """Send stream_opened from a request's context.

        Return the aliases left open in the request's thread and in the shared thread.
        """
        signals.stream_opened.connect(receiver, weak=False)
        self.addCleanup(signals.stream_opened.disconnect, receiver)

        async def request() -> tuple[list[str], list[str]]:
            # The ASGI handler runs each request, the stream's body included, in such a context.
            async with ThreadSensitiveContext():
                self.request_thread = await sync_to_async(threading.get_ident)()
                await signals.asend(signals.stream_opened, signals.StreamInfo, stream=None)
                shared = await self.in_shared_thread(open_aliases)
                await asyncio.gather(*self.tasks)
                return await sync_to_async(open_aliases)(), shared

        # asyncio.run(), as an ASGI server: async_to_sync() would send all sync code to this thread.
        return asyncio.run(request())

    def test_a_receivers_task_queries_outside_the_streams_thread(self):
        threads: list[int] = []

        async def query() -> None:
            await StreamEvent.objects.aexists()
            threads.append(await sync_to_async(threading.get_ident)())

        async def receiver(sender: Any, **kwargs: Any) -> None:  # noqa: ARG001
            self.tasks.append(asyncio.get_running_loop().create_task(query()))

        request, _ = self.send_in_a_request(receiver)
        self.assertEqual(request, [])
        self.assertNotIn(self.request_thread, threads)

    def test_a_sync_receiver_queries_outside_the_streams_thread(self):
        threads: list[int] = []

        def receiver(sender: Any, **kwargs: Any) -> None:  # noqa: ARG001
            connections["direct"].ensure_connection()
            threads.append(threading.get_ident())

        # asend() closes what the receiver opened in the shared thread.
        self.assertEqual(self.send_in_a_request(receiver), ([], []))
        self.assertNotIn(self.request_thread, threads)

    def test_a_receiver_keeps_the_requests_language(self):
        languages: list[str] = []

        def receiver(sender: Any, **kwargs: Any) -> None:  # noqa: ARG001
            languages.append(translation.get_language())

        # LocaleMiddleware activates the language in the request's context.
        with translation.override("de"):
            self.send_in_a_request(receiver)
        self.assertEqual(languages, ["de"])

    def test_close_connection_closes_default_and_the_hub_alias(self):
        for alias in ("default", "direct"):
            connections[alias].ensure_connection()
        signals.close_connection()
        self.assertEqual(open_aliases(), [])

    def test_close_connection_leaves_an_atomic_block_open(self):
        # close() would roll the block back and drop its on_commit callbacks, such as a broadcast.
        with transaction.atomic(), transaction.atomic(using="direct"):
            for alias in ("default", "direct"):
                connections[alias].ensure_connection()
            signals.close_connection()
            self.assertEqual(open_aliases(), ["default", "direct"])
            publish.broadcast("public:news", "hi")
        self.assertTrue(StreamEvent.objects.filter(topic="public:news").exists())


class CloseStreamsArgumentsTest(SimpleTestCase):
    def test_needs_a_user_or_a_session(self):
        with self.assertRaises(ValueError):
            publish.close_streams()

    def test_a_session_without_a_key_has_no_streams(self):
        publish.close_streams(session=SessionStore())


ROLE = "django_pg_sse_test_role"


class HubDatabaseAliasTest(StreamTestCase):
    databases = {"default", "direct"}

    @override_settings(PG_SSE={"DATABASE": "direct"})
    async def test_the_hub_connects_through_its_alias(self):
        hub = hub_module.Hub()
        try:
            query = sql.SQL("SELECT current_setting('application_name')")
            rows = [await hub.live.fetch(query, []), await hub.streams.fetch(query, [])]
        finally:
            await hub.stop()
        self.assertEqual(rows, [[("pg_sse_direct",)]] * 2)

    @override_settings(PG_SSE={"DATABASE": "direct"})
    @stops_the_hub
    async def test_a_broadcast_through_default_reaches_a_hub_on_the_alias(self):
        stream = await self.open_stream("public:news")
        await sync_to_async(publish.broadcast)("public:news", "<p>across aliases</p>")
        self.assertIn("<p>across aliases</p>", await self.next_message(stream))

    @override_settings(PG_SSE={"DATABASE": "direct"})
    async def test_a_hub_on_another_database_is_logged(self):
        # broadcast() writes through default; a hub on another database would hear nothing.
        async def connect_to_postgres() -> hub_module.Connection:
            params: dict[str, Any] = {**hub_module.connect_params(), "dbname": "postgres"}
            return await psycopg.AsyncConnection.connect(**params, autocommit=True)

        with mock.patch.object(hub_module, "_connect", connect_to_postgres):
            with self.assertLogs(hub_module.logger, "ERROR"):
                self.assertTrue(await hub_module._verify_database())  # pyright: ignore[reportPrivateUsage]

    @override_settings(PG_SSE={"DATABASE": "direct"})
    async def test_a_hub_on_the_same_database_logs_nothing(self):
        with self.assertNoLogs(hub_module.logger, "WARNING"):
            self.assertTrue(await hub_module._verify_database())  # pyright: ignore[reportPrivateUsage]

    @override_settings(PG_SSE={"DATABASE": "direct", "QUERY_TIMEOUT": 0.2})
    async def test_a_comparison_that_cannot_finish_reports_it(self):
        async def hang() -> hub_module.Connection:
            await asyncio.sleep(60)
            raise AssertionError

        with mock.patch.object(hub_module, "_connect", hang), self.assertLogs(hub_module.logger, "WARNING"):
            compared = await asyncio.wait_for(hub_module._verify_database(), 5)  # pyright: ignore[reportPrivateUsage]
        self.assertFalse(compared)

    @override_settings(PG_SSE={"DATABASE": "direct"})
    @stops_the_hub
    async def test_the_comparison_does_not_delay_the_first_stream(self):
        # A slow default must not hold LISTEN; streams would time out waiting for the hub.
        async def slow() -> bool:
            await asyncio.sleep(60)
            return True

        with mock.patch.object(hub_module, "_verify_database", slow):
            await asyncio.wait_for(self.open_stream("public:news"), 5)


class HubConnectionOptionsTest(TransactionTestCase):
    databases = {"default", "direct"}

    def run_sql(self, statement: str) -> None:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL(statement).format(sql.Identifier(ROLE)))  # pyright: ignore[reportArgumentType]

    async def test_hub_connections_take_the_libpq_options(self):
        options = {**connection.settings_dict["OPTIONS"], "application_name": "pg-sse-test"}
        hub = hub_module.Hub()
        try:
            with mock.patch.dict(connection.settings_dict, OPTIONS=options):
                query = sql.SQL("SELECT current_setting('application_name')")
                rows = [await hub.live.fetch(query, []), await hub.streams.fetch(query, [])]
        finally:
            await hub.stop()
        self.assertEqual(rows, [[("pg-sse-test",)]] * 2)

    async def test_hub_connections_take_the_configured_role(self):
        await sync_to_async(self.run_sql)("DROP ROLE IF EXISTS {}")
        await sync_to_async(self.run_sql)("CREATE ROLE {} NOLOGIN")
        self.addCleanup(self.run_sql, "DROP ROLE IF EXISTS {}")
        options = {**connection.settings_dict["OPTIONS"], "assume_role": ROLE}
        hub = hub_module.Hub()
        try:
            with mock.patch.dict(connection.settings_dict, OPTIONS=options):
                query = sql.SQL("SELECT current_user")
                rows = [await hub.live.fetch(query, []), await hub.streams.fetch(query, [])]
        finally:
            await hub.stop()
        self.assertEqual(rows, [[(ROLE,)]] * 2)

    @override_settings(PG_SSE={"DATABASE": "direct"})
    async def test_the_role_comes_from_the_hub_alias(self):
        await sync_to_async(self.run_sql)("DROP ROLE IF EXISTS {}")
        await sync_to_async(self.run_sql)("CREATE ROLE {} NOLOGIN")
        self.addCleanup(self.run_sql, "DROP ROLE IF EXISTS {}")
        direct = connections["direct"].settings_dict
        hub = hub_module.Hub()
        try:
            with mock.patch.dict(direct, OPTIONS={**direct["OPTIONS"], "assume_role": ROLE}):
                rows = await hub.live.fetch(sql.SQL("SELECT current_user"), [])
        finally:
            await hub.stop()
        self.assertEqual(rows, [(ROLE,)])
