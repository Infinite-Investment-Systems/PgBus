import asyncio
import contextlib
import dataclasses
import enum
import logging
import sys
import asyncpg
import random
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Union

if sys.version_info < (3, 11, 0):
    from async_timeout import timeout
else:
    from asyncio import timeout  # type: ignore

logger = logging.getLogger(__package__)


class ListenPolicy(str, enum.Enum):
    ALL = "ALL"
    LAST = "LAST"

    def __str__(self) -> str:
        return self.value


@dataclasses.dataclass(frozen=True)
class Timeout:
    __slots__ = ("channel",)

    channel: str


@dataclasses.dataclass(frozen=True)
class Notification:
    __slots__ = ("channel", "payload")

    channel: str
    payload: Optional[str]


ConnectFunc = Callable[[], Coroutine[Any, Any, asyncpg.Connection]]
NotificationOrTimeout = Union[Notification, Timeout]
NotificationHandler = Callable[[NotificationOrTimeout], Coroutine]

NO_TIMEOUT: float = -1


class NotificationListener:
    __slots__ = ("_connect", "_reconnect_delay")

    def __init__(self, connect: ConnectFunc, reconnect_delay: float = 5) -> None:
        self._reconnect_delay = reconnect_delay
        self._connect = connect

    async def run(
        self,
        handler_per_channel: Dict[str, NotificationHandler],
        *,
        policy: ListenPolicy = ListenPolicy.ALL,
        notification_timeout: float = 30,
        max_concurrency_per_channel: int = 1,
    ) -> None:
        queue_per_channel: Dict[str, "asyncio.Queue[Notification]"] = {
            channel: asyncio.Queue() for channel in handler_per_channel.keys()
        }

        read_notifications_task = asyncio.create_task(
            self._read_notifications(
                queue_per_channel=queue_per_channel, check_interval=max(1.0, notification_timeout / 3.0)
            ),
            name=__package__,
        )
        process_notifications_tasks = [
            asyncio.create_task(
                self._process_notifications(
                    channel,
                    notifications=queue_per_channel[channel],
                    handler=handler,
                    policy=policy,
                    notification_timeout=notification_timeout,
                    max_concurrency_per_channel=max_concurrency_per_channel,
                ),
                name=f"{__package__}.{channel}",
            )
            for channel, handler in handler_per_channel.items()
        ]
        try:
            await asyncio.gather(read_notifications_task, *process_notifications_tasks)
        finally:
            await self._cancel_and_await_tasks([read_notifications_task, *process_notifications_tasks])

    @staticmethod
    async def _cancel_and_await_tasks(tasks: "List[asyncio.Task[None]]") -> None:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t

    @staticmethod
    async def _process_notifications(
        channel: str,
        *,
        notifications: "asyncio.Queue[Notification]",
        handler: NotificationHandler,
        policy: ListenPolicy,
        notification_timeout: float,
        max_concurrency_per_channel: int,
    ) -> None:
        max_parallel = max(1, max_concurrency_per_channel)
        inflight: Set["asyncio.Task[None]"] = set()

        async def _invoke(notification: NotificationOrTimeout) -> None:
            try:
                await handler(notification)
            except Exception:
                logger.exception("Failed to handle %s", notification)

        async def _dispatch(notification: NotificationOrTimeout, semaphore: "asyncio.Semaphore") -> None:
            try:
                await _invoke(notification)
            finally:
                semaphore.release()

        semaphore = asyncio.Semaphore(max_parallel)

        try:
            while True:
                try:
                    if notification_timeout == NO_TIMEOUT:
                        notification: NotificationOrTimeout = await notifications.get()
                    else:
                        async with timeout(notification_timeout):
                            notification = await notifications.get()
                except asyncio.TimeoutError:
                    notification = Timeout(channel)

                if policy == ListenPolicy.LAST:
                    # Collapse backlog to the latest item when configured for LAST.
                    while True:
                        try:
                            notification = notifications.get_nowait()
                        except asyncio.QueueEmpty:
                            break

                await semaphore.acquire()
                task = asyncio.create_task(_dispatch(notification, semaphore), name=f"{__package__}.{channel}")
                inflight.add(task)
                task.add_done_callback(inflight.discard)
        finally:
            await NotificationListener._cancel_and_await_tasks(list(inflight))

    async def _read_notifications(
        self, queue_per_channel: Dict[str, "asyncio.Queue[Notification]"], check_interval: float
    ) -> None:
        failed_connect_attempts = 1
        while True:
            try:
                connection = await self._connect()
                failed_connect_attempts = 1
                try:

                    await connection.execute(f"SET application_name TO 'PgBusHost-{'-'.join(queue_per_channel.keys())}'")

                    for channel, queue in queue_per_channel.items():
                        await connection.add_listener(channel, self._get_push_callback(queue))

                    while not connection.is_closed():
                        await asyncio.sleep(check_interval)
                finally:
                    await asyncio.shield(connection.close())
            except Exception:
                logger.exception(f"Connection was lost or not established - attempted {failed_connect_attempts} times....")
                backoff = self._reconnect_delay * (2 ** (failed_connect_attempts - 1))
                backoff = min(backoff, 60)
                backoff *= random.uniform(0.5, 1.5)
                await asyncio.sleep(backoff)
                failed_connect_attempts += 1
                if failed_connect_attempts > 10:
                    raise Exception("Too many failed connection attempts")

    @staticmethod
    def _get_push_callback(queue: "asyncio.Queue[Notification]") -> Callable[[Any, Any, Any, Any], None]:
        def _push(_: Any, __: Any, channel: Any, payload: Any) -> None:
            queue.put_nowait(Notification(channel, payload))

        return _push
