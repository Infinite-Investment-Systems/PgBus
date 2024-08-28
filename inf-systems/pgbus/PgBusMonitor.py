from pydantic import BaseModel
import datetime
from typing import Optional, Union, List, Any, Dict, Tuple
from src.core.logging_util import *
import asyncpg


class PgBusRegistration(BaseModel):
    id: int
    queue_name: str
    message_type: str
    queued: int
    processed: int
    dead_letter: int
    oldest_age: Optional[str]
    topic_name: Optional[str]
    handler_function: str
    run_as_subprocess: bool
    is_active: bool


class PgBusQueued(BaseModel):
    key: str
    queue_name: str
    message_type: str
    payload: str
    priority: int
    correlation_key: Optional[str]
    scheduled_at: datetime.datetime
    dequeued_at: Optional[datetime.datetime]


class PgBusProcessed(PgBusQueued):
    error: Optional[str]
    # Logically this can't be an optional field --only for test means
    dequeued_at: Optional[datetime.datetime]
    processed_at: datetime.datetime


class PgBusDeadLetter(PgBusQueued):
    error: str
    scheduled_at: Optional[datetime.datetime]
    dequeued_at: datetime.datetime
    captured_at: datetime.datetime


class PgBusMonitor:
    def __init__(self, pg_pool: asyncpg.Pool):
        self.pool = pg_pool
        self._logger = logging.getLogger('PgBusMonitor')

    def _get_connection(self):
        return self.pool.acquire()

    async def get_queue_registrations(self, duration: str) -> List[PgBusRegistration]:

        # A timestamp according to the chosen duration to search based on that
        # Taking 1 day as the default
        td = datetime.timedelta(days=1)
        match duration:
            case "1 day":
                td = datetime.timedelta(days=1)
            case "3 days":
                td = datetime.timedelta(days=3)
            case "A week ago":
                td = datetime.timedelta(days=7)
            case "A month ago":
                td = datetime.timedelta(days=30)
            # For testing purposes
            case "Of all time":
                td = datetime.timedelta(days=365)
            case _:
                self._logger.error("Unknown duration %s", duration)
        current = datetime.datetime.utcnow()
        looking_at = current - td

        query = f"""
        WITH
        queued_messages AS (
            SELECT qm.queue_name, qm.message_type, COUNT(*) AS queued
            FROM pgbus.message qm
            WHERE qm.scheduled_at >= $1
            GROUP BY qm.queue_name, qm.message_type
        ),
        processed_messages AS (
            SELECT pm.queue_name, pm.message_type, COUNT(*) AS processed
            FROM pgbus.message_log pm
            WHERE pm.scheduled_at >= $1
            GROUP BY pm.queue_name, pm.message_type
        ),
        dead_letter_messages AS (
            SELECT dlm.queue_name, dlm.message_type, COUNT(*) AS dead_letter
            FROM pgbus.dead_letter dlm
            WHERE dlm.scheduled_at >= $1
            GROUP BY dlm.queue_name, dlm.message_type
        ),
        oldest_message_age AS (
            SELECT queue_name, message_type, MAX(AGE(CURRENT_TIMESTAMP, scheduled_at))::VARCHAR AS age
            FROM pgbus.message
            GROUP BY queue_name, message_type
        )
        SELECT
            q.id AS id,
            q.queue_name AS queue_name,
            q.message_type AS message_type,
            old.age AS oldest_age,
            COALESCE(qm.queued, 0) AS queued,
            COALESCE(pm.processed, 0) AS processed,
            COALESCE(dlm.dead_letter, 0) AS dead_letter,
            q.topic_name AS topic_name,
            q.handler_function AS handler_function,
            q.run_as_subprocess AS run_as_subprocess,
            q.is_active AS is_active
        FROM pgbus.registration q
        LEFT JOIN queued_messages qm ON qm.queue_name = q.queue_name AND qm.message_type = q.message_type
        LEFT JOIN processed_messages pm ON pm.queue_name = q.queue_name AND pm.message_type = q.message_type
        LEFT JOIN dead_letter_messages dlm ON dlm.queue_name = q.queue_name AND dlm.message_type = q.message_type
        LEFT JOIN oldest_message_age old ON old.queue_name = q.queue_name AND old.message_type = q.message_type
        """
        try:
            async with self._get_connection() as conn:
                registrations = await conn.fetch(query, looking_at)
                result = [PgBusRegistration(**r) for r in registrations]
                return result
        except Exception as e:
            self._logger.error("get_queue_registrations: ", e)
            raise

    async def get_processed_messages(self, queue_name: str, message_type: str,
                                     from_date: str, to_date: str) -> List[PgBusProcessed]:

        from_dt = datetime.datetime.fromisoformat(from_date.rstrip('Z')).date()
        to_dt = datetime.datetime.fromisoformat(to_date.rstrip('Z')).date()

        query = f"""
        SELECT *
        FROM pgbus.message_log AS t
        WHERE t.queue_name=$1 AND t.message_type=$2 AND t.scheduled_at::date >= $3 AND t.scheduled_at::date <= $4
        ORDER BY t.key DESC
        """
        try:
            async with self._get_connection() as conn:
                messages = await conn.fetch(query, queue_name, message_type, from_dt, to_dt)
                res = [PgBusProcessed(**message) for message in messages]
                return res
        except Exception as e:
            self._logger.error("get_processed_messages: ", e)
            raise

    async def get_messages_in(self, message_state: str, queue_name: str, message_type: str) \
            -> List[Union[PgBusQueued, PgBusDeadLetter]]:
        match message_state:
            case "queued":
                records = await self._get_messages("pgbus.message", queue_name, message_type)
                res = [PgBusQueued(**record) for record in records]
            case "dead_letter":
                records = await self._get_messages("pgbus.dead_letter", queue_name, message_type)
                res = [PgBusDeadLetter(**record) for record in records]
            case _:
                self._logger.debug(f'message state is not queued or dead letter. its {message_state}')
                return
        return res

    async def _get_messages(self, table_name: str, queue_name: str, message_type: str) -> List[Dict[str, Any]]:
        query = f"""
        SELECT *
        FROM {table_name} AS t
        WHERE t.queue_name=$1 AND t.message_type=$2
        ORDER BY t.key DESC
        """
        try:
            async with self._get_connection() as conn:
                messages = await conn.fetch(query, queue_name, message_type)
                return messages
        except Exception as e:
            self._logger.error("_get_messages: ", e)
            raise

    async def delete_messages(self, queue_name: str, message_type: str) -> None:
        query = f"""
        DELETE FROM pgbus.dead_letter
        WHERE queue_name=$1 AND message_type=$2
        """
        try:
            async with self._get_connection() as conn:
                await conn.execute(query, queue_name, message_type)
                self._logger.debug(f'messages deleted successfully')
        except Exception as e:
            self._logger.error("delete_messages: ", e)
            raise

    async def push_messages_to_queue(self, queue_name: str, message_type: str) -> None:
        insert_query = f"""
        INSERT INTO pgbus.message (key, queue_name, message_type, payload, priority, correlation_key, scheduled_at)
        SELECT key, queue_name, message_type, payload, priority, correlation_key, CURRENT_TIMESTAMP
        FROM pgbus.dead_letter
        WHERE queue_name=$1 AND message_type=$2
        """

        delete_query = f"""
        DELETE FROM pgbus.dead_letter
        WHERE queue_name=$1 AND message_type=$2
        """

        try:
            async with self._get_connection() as conn:
                async with conn.transaction():
                    await conn.execute(insert_query, queue_name, message_type)
                    self._logger.debug(f'messages pushed successfully')
                    await conn.execute(delete_query, queue_name, message_type)
                    self._logger.debug(f'messages deleted successfully')
        except Exception as e:
            self._logger.error("push_messages_to_queue: ", e)
            raise

    async def get_elapsed_time(self, queue_name: str, message_type: str, start_time: str, end_time: str, aggregation: str) \
            -> List[Dict[str, Any]]:

        from_dt = datetime.datetime.fromisoformat(start_time.rstrip('Z')).date()
        to_dt = datetime.datetime.fromisoformat(end_time.rstrip('Z')).date()

        time_difference_in_days = (to_dt - from_dt).days
        if time_difference_in_days == 0:
            time_difference_in_days = 1

        aggregation_method = 'AVG'
        if aggregation == "Average":
            aggregation_method = "AVG"
        elif aggregation == 'Maximum':
            aggregation_method = "MAX"
        elif aggregation == 'Summation':
            aggregation_method = "SUM"
        else:
            self._logger.error(f'aggregation method is not supported: {aggregation}')

        query = f"""
        WITH
        intervaled AS (
            SELECT 
                date_trunc('minute', scheduled_at) - interval '1 minute' * (EXTRACT(MINUTE FROM scheduled_at)::int % {5 * time_difference_in_days}) AS time_interval,
                mlog.scheduled_at AS scheduled_at,
                mlog.processed_at AS processed_at,
                mlog.dequeued_at AS dequeued_at
            FROM pgbus.message_log AS mlog
            WHERE scheduled_at IS NOT NULL AND queue_name=$1 AND message_type=$2
        )
        SELECT
        t.time_interval,
        {aggregation_method}(EXTRACT(EPOCH FROM (t.dequeued_at - t.scheduled_at)) / 60) AS elapsed_time_to_dequeue,
        {aggregation_method}(EXTRACT(EPOCH FROM (t.processed_at - t.dequeued_at)) / 60) AS elapsed_time_to_process
        FROM intervaled AS t
        WHERE t.scheduled_at::date >= $3 AND t.scheduled_at::date <= $4
        GROUP BY time_interval
        ORDER BY time_interval
        """
        try:
            async with self._get_connection() as conn:
                elapsed_times = await conn.fetch(query, queue_name, message_type, from_dt, to_dt)
                self._logger.debug(f'{query} was successfully executed')
                return elapsed_times
        except Exception as e:
            self._logger.error("get_elapsed_time: ", e)
            raise

    async def toggle_is_active(self, queue_name: str, message_type: str, is_active: bool) -> None:
        query = f"""
        UPDATE pgbus.registration SET is_active = $3 WHERE queue_name=$1 AND message_type=$2
        """
        try:
            async with self._get_connection() as conn:
                await conn.execute(query, queue_name, message_type, is_active)
                self._logger.debug(f'{queue_name}:{message_type} is_active state changed to {is_active}')
        except Exception as e:
            self._logger.error("toggle_is_active: ", e)
            raise
