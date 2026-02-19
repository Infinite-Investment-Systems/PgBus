from pydantic import BaseModel, Field, StringConstraints,ConfigDict
from typing_extensions import Annotated
from pydantic_settings import BaseSettings, SettingsConfigDict
import datetime


class PgBusMessage(BaseModel):
    """
    The data structure for storing messages
    """
    model_config = ConfigDict(slots=True)
    key: Annotated[str|None, StringConstraints(max_length=60, strip_whitespace=True)] = None
    queue_name: Annotated[str|None, StringConstraints(max_length=60, strip_whitespace=True)] = None
    message_type: Annotated[str|None, StringConstraints(max_length=128, strip_whitespace=True)] = None
    payload: str
    priority: int = Field(default=0)
    correlation_key: str | None = None
    scheduled_at: datetime.datetime | None = None
    dequeued_at: datetime.datetime | None = None


class PgBusRegistration(BaseModel):
    """
    The data structure for registering queues, topics, and processing functions
    """
    model_config = ConfigDict(slots=True)
    id: int|None = None
    queue_name: str
    message_type: str
    topic_name: str|None = None
    description: str|None = None
    handler_function: str
    run_as_subprocess: bool = Field(default=False)
    is_active: bool = Field(default=True)
    prevent_duplication: bool = Field(default=True)
    inserted_at: datetime.datetime|None = None
    updated_at: datetime.datetime|None = None
    keep_log: bool|None = Field(default=False)


class PgBusHostSettings(BaseSettings):
    retry_count: int = Field(default=3)
    retry_wait_time: float = Field(default=2)
    timeout_in_minutes: int = Field(default=1)
    handler_object_cached_duration: int = Field(default=0)
    max_workers_per_queue: int = Field(default=1)

    model_config = SettingsConfigDict(env_file_encoding='utf-8',
                                      env_prefix="",
                                      case_sensitive=False)


class NoRetryException(Exception):
    """
    Custom exception class to signal the consumer not to attempt any retries
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
