import uuid6
import logging
import logging.handlers
import asyncpg
from infsystems.pgbus.bus_model import PgBusRegistration,PgBusMessage
from infsystems.pgbus.BaseBus import BasePgBus


class PgBus(BasePgBus):
    """
    Implementation of the BasePgBus
    """
    def __init__(self, pg_pool: asyncpg.Pool):
        self.pool = pg_pool
        self._logger = logging.getLogger()
        if not self._logger.handlers:
            self._logger = logging.getLogger('infinite.PgBus')

        self.registration_map: dict[str, PgBusRegistration] = dict()

    @classmethod
    async def from_connection_string(cls,
                                     connection_string: str,
                                     pool_name: str = 'PgBus-Pool',
                                     min_pool_size: int | None = 1,
                                     max_pool_size: int | None = 10) -> "PgBus":
        """
        Factory method to create an instance of this class with connection string
        Alternative is to construct with an injected asyncpg.Pool instance.
        Args:
            connection_string: Postgres connection string. Please refer to asyncpg documentation for the format.
            pool_name: Name of the connection pool
            min_pool_size: Minimum number of connections in the pool
            max_pool_size: Maximum number of connections in the pool
        Returns:
            An instance of PgBus
        """
        pool = await asyncpg.create_pool(connection_string,
                                         statement_cache_size=0,
                                         server_settings={'application_name': f'{pool_name}'},
                                         min_size=min_pool_size,
                                         max_size=max_pool_size)
        self = cls(pg_pool=pool)
        return self

    async def publish(self,
                      topic_name: str,
                      payload: str,
                      priority: int = 0,
                      correlation_key: str|None = None,
                      conn: asyncpg.connection.Connection = None
                      ) -> None:

        needs_new_conn = not conn
        if needs_new_conn:
            conn = await self.pool.acquire()
        try:
            stmt = """SELECT queue_name, message_type
                        FROM pgbus.registration
                        WHERE topic_name=$1"""
            rows = await conn.fetch(stmt, topic_name)
            if not rows:
                self._logger.debug(f'There are no registered queues for topic {topic_name} !')
                return

            queues_and_types = [(row['queue_name'], row['message_type']) for row in rows]
            async with conn.transaction():
                for pair in queues_and_types:
                    await self.send(
                        queue_name=pair[0],
                        message_type=pair[1],
                        payload=payload,
                        priority=priority,
                        correlation_key=correlation_key,
                        conn=conn)

        except Exception as ex:
            self._logger.exception(ex)
            raise RuntimeError(f"Failed to publish the message to topic {topic_name}!") from ex
        finally:
            if needs_new_conn and conn:
                await conn.close()

    async def _insert_message(self,
                              message: PgBusMessage,
                              conn: asyncpg.connection.Connection,
                              minutes_in_future: int = 0):
        if len(message.queue_name) > 60:
            raise ValueError(f'queue_name {message.queue_name} is longer than 60 characters!')
        if len(message.message_type) > 128:
            raise ValueError(f'message_type {message.message_type} is longer than 128 characters!')
        if len(message.key) > 60:
            raise ValueError(f'key {message.key} is longer than 60 characters!')

        stmt = f"""INSERT INTO pgbus.message(
                   key 
                  ,queue_name
                  ,payload
                  ,message_type
                  ,priority
                  ,correlation_key
                  ,scheduled_at) 
                VALUES($1, $2, $3, $4, $5, $6
                  ,STATEMENT_TIMESTAMP() + ($7 * interval '1 minute') ) """
        await conn.fetchval(stmt,
                            message.key,
                            message.queue_name,
                            message.payload,
                            message.message_type,
                            message.priority,
                            message.correlation_key,
                            minutes_in_future)

        self._logger.debug(f"message has been pushed to {message.message_type}@{message.queue_name}"
                           f"with key {message.key}")

    async def _get_registration(self, queue_name: str, message_type: str) -> PgBusRegistration:
        map_key = self.get_registration_map_key(queue_name, message_type)
        if map_key not in self.registration_map:
            # If the new queue has been registered on a different endpoint, we will requery to catch up.
            await self.build_registration_map()
            if map_key not in self.registration_map:
                raise ValueError(f'message_type@queue_name of {map_key} has not been registered yet!')
        return self.registration_map[map_key]


    # @with_optional_connection()
    async def send(self, queue_name: str, message_type: str, payload: str, priority: int|None = 0,
                   correlation_key: str|None = None,
                   conn: asyncpg.connection.Connection|None = None) -> bool:
        reg = await self._get_registration(queue_name, message_type)

        if not reg.is_active:
            self._logger.warning(f'message_type of {reg.message_type} for {reg.queue_name} queue is not active!')

        needs_new_conn = not conn
        if needs_new_conn:
            conn = await self.pool.acquire()

        try:

            if reg.prevent_duplication and await self._find_duplicates(
                    queue_name=queue_name,
                    payload=payload,
                    message_type = message_type,
                    conn=conn,
                    for_deferral=False):

                self._logger.warning(f'Found existing duplicated messages in queue'
                                     f'{message_type}@{queue_name} with the same payload!')
                return False

            message = PgBusMessage(
                key=str(uuid6.uuid7()),
                queue_name=queue_name,
                message_type=message_type,
                payload=payload,
                priority=priority,
                correlation_key=correlation_key)

            self._logger.debug(f'Sending {message.model_dump_json()}')

            async with conn.transaction():
                stmt = "SELECT pg_notify($1, $2);"
                await conn.fetchval(stmt, queue_name, message.key)
                await self._insert_message(message=message, conn=conn, minutes_in_future=0)

                self._logger.info("inserted one message into queue")

            return True

        except Exception as ex:
            self._logger.exception(ex)
            raise RuntimeError("Failed to enqueue the message!") from ex
        finally:
            if needs_new_conn and conn:
                await conn.close()

    # @with_optional_connection()
    async def defer(self,
                    message: PgBusMessage,
                    minutes_in_future: int,
                    conn: asyncpg.connection.Connection = None) -> bool:
        needs_new_conn = not conn
        if needs_new_conn:
            conn = await self.pool.acquire()
        try:
            async with conn.transaction():

                reg = await self._get_registration(message.queue_name, message.message_type)

                if reg.prevent_duplication and await self._find_duplicates(
                        queue_name=message.queue_name,
                        payload=message.payload,
                        message_type=message.message_type,
                        conn=conn,
                        for_deferral=True
                        ):
                    self._logger.info(
                        f"message {message.message_type}@{message.queue_name} has been already deferred!")
                    return False

                message_copy = message.model_copy(deep=True)
                message_copy.key = str(uuid6.uuid7())
                await self._insert_message(
                    message=message_copy,
                    conn=conn,
                    minutes_in_future=minutes_in_future)

                self._logger.info(
                    f"message {message.message_type}@{message_copy.queue_name} has been deferred by "
                    f"{minutes_in_future} minutes")
        except Exception as ex:
            self._logger.exception(ex)
            raise RuntimeError("Failed to defer the message!") from ex
        finally:
            if needs_new_conn and conn:
                await conn.close()

        return True

    @staticmethod
    def get_registration_map_key(queue_name, message_type):
        return f'{message_type}@{queue_name}'

    async def build_registration_map(self):
        stmt = f"""SELECT *
                FROM pgbus.registration"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(stmt)
            for row in rows:
                reg = PgBusRegistration(**row)
                key = self.get_registration_map_key(reg.queue_name, reg.message_type)
                self.registration_map[key] = reg
        self._logger.debug(f'Loaded {len(rows)} registered queues.')

    async def _find_duplicates(self,
                               queue_name: str,
                               payload: str,
                               message_type: str,
                               conn: asyncpg.connection.Connection,
                               for_deferral: bool) -> bool:
        self._logger.debug('Checking for active duplicated entries...')
        defer_time_check = ''
        if for_deferral:
            defer_time_check = ' AND scheduled_at > STATEMENT_TIMESTAMP()'

        stmt = f"""SELECT 1
                    FROM pgbus.message 
                    WHERE queue_name = $1
                    AND payload = $2 
                    AND message_type = $3 {defer_time_check}
                    LIMIT 1"""
        row = await conn.fetchrow(stmt, queue_name, payload, message_type)
        return row is not None
