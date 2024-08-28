import asyncpg
import abc
from ..bus.bus_model import *


class BasePgBus(metaclass=abc.ABCMeta):
    """
    Abstract class / Common interface for clients that are performing send, publish, and defer
    """

    @abc.abstractmethod
    async def publish(self,
                      topic_name: str,
                      payload: str,
                      priority: int = 0,
                      correlation_key: Optional[str] = None,
                      conn: asyncpg.connection.Connection = None
                      ) -> None:
        """
        Publish an "EVENT" to a topic.Think of this as an event that happened. (Fan-Out pattern)
        If no queue is associated with the topic, nothing will happen as there is no subscriber/consumer of the event
        has been registered before.
        You need to register a queue for the subscriber by adding a record in pgbus_registration table.
        Many subscribers can be configured for the topic and each queue will receive a copy of the message.
        Args:
            topic_name: Topic Name (Event name)
            payload: Message body as string. You can serialize your object as json.
            priority: Default 0, by increasing this you can prioritize the order of message processing.
            correlation_key: To correlate messages that are part of a given workflow.
            conn: Postgres connection object to ensure the integrity of database transaction with message processing.
                  Transaction consistency can be only ensured for queues that run_as_subprocess is set to False.
        """
        raise NotImplementedError('This is an abstract class.')

    @abc.abstractmethod
    async def send(self, queue_name: str, message_type: str, payload: str, priority: int = 0,
                   correlation_key: Optional[str] = None, conn: asyncpg.connection.Connection = None) -> None:
        """
        Send a "COMMAND" to a given queue. Think of this as a request for execution. (Fire-and-Forget pattern)
        You need to register the queue by adding a record in pgbus_registration table.
        Args:
            queue_name: Name of the queue (Command name)
            message_type: Optionally specifies the type of message
            payload: Message body as string. You can serialize your object as json.
            priority: Default 0, by increasing this you can prioritize the order of message processing.
            correlation_key: To correlate messages that are part of a given workflow.
            conn: Postgres connection object to ensure the integrity of database transaction with message processing.
                  Transaction consistency can be only ensured for queues that run_as_subprocess is set to False.
        """
        raise NotImplementedError('This is an abstract class.')

    @abc.abstractmethod
    async def defer(self,
                    message: PgBusMessage,
                    minutes_in_future: int,
                    conn: asyncpg.connection.Connection = None) -> None:
        """
        Defer a message to be processed in the future. Since We rely on Postgres NOTIFY and LISTEN features,
        and we can't broadcast in future without running a CRON, we can't guarantee the deferral shorter than 1 minute.
        To support this feature, we will scan the pgbus_message table every minute, even though we have not received
        any message.
        Args:
            message: The message object that needs to be deferred.
            minutes_in_future: Number of minutes in the future.
            conn: Postgres connection object to ensure the integrity of database transaction with message processing.
                  Transaction consistency can be only ensured for queues that run_as_subprocess is set to False.
        """
        raise NotImplementedError('This is an abstract class.')

