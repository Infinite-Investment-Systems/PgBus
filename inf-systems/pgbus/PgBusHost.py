import contextlib
import gc
import logging.handlers
import inspect
import logging.handlers
from typing import List
import asyncpg_listen
import psutil
from ..bus.PgBusSubProcessFork import *


class PgBusHost:
    def __init__(self,
                 bus: PgBus,
                 connection_string_bypassed_pgbouncer: str,
                 setting: PgBusHostSettings | None = None,
                 logger: logging.Logger | None = None):

        self._connection_string = connection_string_bypassed_pgbouncer

        if not logger:
            self._logger = logging.getLogger()
            if not self._logger.handlers:
                self._logger = logging.getLogger('infinite.PgBus')
        else:
            self._logger = logger

        if not setting:
            self._setting = PgBusHostSettings()
        else:
            self._setting = setting

        self._bus = bus
        self._termination_requested = False
        self._reconnect_delay = 5.0
        self._listener_map = dict()
        self._puller_tasks = list()
        self._host_pool: asyncpg.Pool | None = None
        self.status: str | None = None

    def _get_process_memory(self) -> float:
        try:
            process = psutil.Process()
            return process.memory_info().rss / (1024 ** 2)
        except (Exception,):
            self._logger.exception(f"Error in calling memory_info", exc_info=True)
            return 0

    async def _get_registration_for_message(self, message: PgBusMessage) -> PgBusRegistration:
        map_key = PgBus.get_registration_map_key(message.queue_name, message.message_type)
        if map_key not in self._bus.registration_map:
            await self._bus.build_registration_map()
            if map_key not in self._bus.registration_map:
                raise NoRetryException(f'The message_type@queue_name "{map_key}" is not registered!')
        return self._bus.registration_map[map_key]

    async def _process_message(self, message: PgBusMessage, conn: asyncpg.connection.Connection) -> None:
        self._logger.info(f'processing {message.payload} from {message.queue_name}....')

        registration = await self._get_registration_for_message(message)

        if registration.run_as_subprocess:
            context = PgBusHandlerContext(setting=self._setting)
            sub_processor = PgBusSubProcessFork()
            await sub_processor.run(context=context,
                                    message=message,
                                    handler_function=registration.handler_function)
        else:
            processor = locate(registration.handler_function)
            context = PgBusHandlerContext(setting=self._setting,
                                          bus=self._bus,
                                          shared_pg_connection=conn)
            # noinspection PyCallingNonCallable
            await processor(context=context, message=message)
        self._logger.debug(f'Finished calling {registration.handler_function}.')

    async def stop(self):
        self._termination_requested = True
        for t in self._puller_tasks:
            if not t.done():
                t.cancel()
        for t in self._puller_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._puller_tasks = []
        if not self._host_pool:
            self._logger.warning('Start had not been called successfully before!')
            return
        try:
            await self._host_pool.close()
        except (Exception,):
            pass

    async def _add_to_message_log(self,
                                  message: PgBusMessage,
                                  conn: asyncpg.connection.Connection,
                                  error_message: str|None = None):

        registration = await self._get_registration_for_message(message)

        if not registration.keep_log:
            return
        

        

        await conn.execute(
            """INSERT INTO pgbus.message_log(key, queue_name, message_type, payload, 
                                            priority, correlation_key, scheduled_at, dequeued_at, processed_at, error) 
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, STATEMENT_TIMESTAMP(), $9)""",
            message.key, message.queue_name, message.message_type,
            message.payload, message.priority, message.correlation_key, message.scheduled_at,
            message.dequeued_at, error_message)

    async def _delete_message(self,
                              message: PgBusMessage,
                              conn: asyncpg.connection.Connection):
        await conn.execute(
            """DELETE FROM pgbus.message WHERE key=$1""",
            message.key)
        self._logger.debug(f'Message with key of {message.key} has been deleted.')

    async def _move_to_dead_letter(self,
                                   message: PgBusMessage,
                                   error_message: str,
                                   conn: asyncpg.connection.Connection):
        await conn.execute(
            """INSERT INTO pgbus.dead_letter(key, queue_name, message_type, payload, 
                                        priority, correlation_key, error, scheduled_at, captured_at, dequeued_at) 
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, STATEMENT_TIMESTAMP(), $9)""",
            str(uuid6.uuid7()), message.queue_name, message.message_type,
            message.payload, message.priority, message.correlation_key,
            error_message, message.scheduled_at, message.dequeued_at)
        self._logger.warning(
            f"message {message.payload} has been pushed to dead-letter queue for "
            f"{message.queue_name} with key {message.message_type}")
        await self._add_to_message_log(message, conn, error_message)
        await self._delete_message(message, conn)

    async def move_from_dead_letter(self,
                                    queue_name: str | None,
                                    message_type: str | None):

        async with self._bus.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """INSERT INTO pgbus.message(
                    key, queue_name, message_type, payload, priority, correlation_key, scheduled_at)
                    SELECT key, queue_name, message_type, payload, priority, correlation_key, scheduled_at
                    FROM pgbus.dead_letter
                    WHERE (queue_name=$1 OR $1 IS NULL) AND (message_type=$2 OR $2 IS NULL)""",
                    queue_name, message_type)

                await conn.execute(
                    """DELETE FROM pgbus.dead_letter 
                    WHERE (queue_name=$1 OR $1 IS NULL) AND (message_type=$2 OR $2 IS NULL)""",
                    queue_name, message_type)

    async def _pull_and_lock(self,
                             queue_name: str,
                             conn: asyncpg.connection.Connection) -> PgBusMessage | None:

        self.status = f'Last pull at: {datetime.datetime.now()}'

        stmt = """WITH msg AS (
                SELECT 
                     m.key
                    ,m.queue_name
                    ,m.message_type
                    ,m.payload
                    ,m.priority
                    ,m.correlation_key 
                    ,m.scheduled_at 
                FROM pgbus.message m
                INNER JOIN pgbus.registration r
                ON r.queue_name=m.queue_name AND 
                    r.message_type=m.message_type  
                WHERE
                m.queue_name = $1 
                AND r.is_active = TRUE
                AND m.scheduled_at <= STATEMENT_TIMESTAMP()
                ORDER BY m.priority DESC, m.key 
                FOR UPDATE OF m SKIP LOCKED 
                LIMIT 1)
            UPDATE pgbus.message
            SET dequeued_at = STATEMENT_TIMESTAMP() 
            FROM msg
            WHERE pgbus.message.key = msg.key
            RETURNING msg.*,STATEMENT_TIMESTAMP()::TIMESTAMP WITHOUT TIME ZONE AS dequeued_at"""

        row = await conn.fetchrow(stmt, queue_name)
        if not row:
            self._logger.debug(f'No messages were found in {queue_name}')
            return None
        self._logger.debug(f'Pulled the following message: {row}')
        return PgBusMessage.construct(**row)

    async def register(self, registration: PgBusRegistration) -> None:

        proc_func = locate(registration.handler_function)
        # noinspection PyTypeChecker
        func_signature = inspect.signature(proc_func)
        param_dict = dict(func_signature.parameters)
        if ('context' not in param_dict or
                'message' not in param_dict or
                'PgBusHandlerContext' not in str(param_dict['context']) or
                'PgBusMessage' not in str(param_dict['message'])):
            raise ValueError(('The handler_function should include an async method with the following parameters: '
                              'context: PgHandlerContext, message: PgBusMessage'))

        async with self._bus.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO pgbus.registration
                        (queue_name, message_type, topic_name, description, handler_function, 
                        run_as_subprocess, prevent_duplication, keep_log)
                    VALUES($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (queue_name, message_type)
                    DO UPDATE SET topic_name=$3, description=$4, handler_function=$5,
                                  run_as_subprocess=$6, prevent_duplication=$7, keep_log=$8""",
                registration.queue_name,
                registration.message_type,
                registration.topic_name,
                registration.description,
                registration.handler_function,
                registration.run_as_subprocess,
                registration.prevent_duplication,
                registration.keep_log)
            self._logger.debug(f'Registration for {registration.queue_name} completed!')

    async def _remove_unregistered_queue(self, queue_list: List[str]):
        await self._bus.build_registration_map()

        for queue_name in queue_list[:]:
            found = False
            for reg in self._bus.registration_map.values():
                if reg.queue_name == queue_name:
                    found = True
                    break
            if not found:
                self._logger.warning(f'Queue {queue_name} has not been registered before!')
                queue_list.remove(queue_name)

    async def start(self, queue_list: List[str]):

        if self._setting.timeout_in_minutes <= 0:
            raise ValueError('timeout_in_minutes should be at least one minute!')

        # Eliminating queue name in the list that is not registered.
        await self._remove_unregistered_queue(queue_list=queue_list)

        if not queue_list:
            self._logger.warning('There was no registered queues in the provided list !')
            return

        self._host_pool = await asyncpg.create_pool(
            self._connection_string,
            statement_cache_size=0,
            server_settings={'application_name': 'PgBusHost-Pool'},
            min_size=len(queue_list),
            max_size=len(queue_list)+1,
            timeout=5,
            command_timeout=10,
            max_inactive_connection_lifetime=self._setting.timeout_in_minutes * 120)

        self._logger.info(f'Starting the listeners for {queue_list} queues.')

        # to process existing messages in the queue which were posted when no active listener existed
        puller_tasks = [self._process_messages(queue_name) for queue_name in queue_list]
        await asyncio.gather(*puller_tasks)

        listener = asyncpg_listen.NotificationListener(self._host_pool.acquire)

        self._logger.debug(f'Starting {len(queue_list)} listeners now...')

        await listener.run(
            {q: self._create_notification_handler(q) for q in queue_list},
            policy=asyncpg_listen.ListenPolicy.LAST,
            notification_timeout=self._setting.timeout_in_minutes * 60)

    def _create_notification_handler(
            self,
            queue_name: str) -> asyncpg_listen.NotificationHandler:
        _queue_name = queue_name

        async def notification_handler(notification: asyncpg_listen.Notification) -> None:
            if notification.channel != _queue_name:
                raise Exception(f'notification for {notification.channel} is dispatched for {_queue_name} handler!')
            if isinstance(notification, asyncpg_listen.Timeout):
                self._logger.debug(notification)

            await self._process_messages(_queue_name)
            self._logger.debug(f"Notification for {queue_name} has been received.")

        return notification_handler

    async def _process_messages(self, channel: str):
        async with self._bus.pool.acquire() as conn:
            # looping to process all the existing messages in the queue
            while not self._termination_requested:
                async with conn.transaction():

                    # fetch the top row and lock, skip it is locked by another session and go to the next
                    message = await self._pull_and_lock(channel, conn)

                    if not message:
                        return

                    max_num_attempts = 1 if not self._setting.retry_count else self._setting.retry_count + 1
                    attempt = 0
                    while True:
                        attempt += 1
                        try:
                            mem1 = self._get_process_memory()
                            self._logger.debug(f"Total process memory (Mb) before handler execution : {mem1}")
                            message_copy = message.model_copy(deep=True)

                            await self._process_message(message_copy, conn)
                            await self._add_to_message_log(message, conn)
                            await self._delete_message(message, conn)

                            # successful completion of message processing, break the retry loop and go back to
                            # the outer loop to process more messages
                            break
                        except NoRetryException:
                            await self._move_to_dead_letter(message, traceback.format_exc(), conn)
                            break
                        except NotImplementedError:
                            await self._move_to_dead_letter(message, traceback.format_exc(), conn)
                            break
                        except Exception as ex:
                            if attempt == max_num_attempts:
                                await self._move_to_dead_letter(message, traceback.format_exc(), conn)
                                break
                            else:
                                self._logger.exception(f"Attempt #{attempt} failed: {str(ex)}", exc_info=True)
                                if self._termination_requested:
                                    self._logger.warning('Termination has been requested, returning.')
                                    return
                                await asyncio.sleep(self._setting.retry_wait_time ** attempt)
                        finally:
                            del gc.garbage[:]
                            gc.collect()
                            mem2 = self._get_process_memory()
                            self._logger.info(f"Change in process memory (Mb) after handler execution : {mem2 - mem1}")
