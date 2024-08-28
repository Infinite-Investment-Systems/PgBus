from pydantic import BaseModel, Field, StringConstraints
from typing_extensions import Annotated
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
import datetime


class PgBusMessage(BaseModel):
    """
    The data structure for storing messages
    """
    key: Annotated[Optional[str], StringConstraints(max_length=60, strip_whitespace=True)] = None
    queue_name: Annotated[Optional[str], StringConstraints(max_length=60, strip_whitespace=True)] = None
    message_type: Annotated[Optional[str], StringConstraints(max_length=128, strip_whitespace=True)] = None
    payload: str
    priority: int = Field(default=0)
    correlation_key: str | None = None
    scheduled_at: datetime.datetime | None = None
    dequeued_at: datetime.datetime | None = None


class PgBusRegistration(BaseModel):
    """
    The data structure for registering queues, topics, and processing functions
    """
    id: Optional[int] = None
    queue_name: str
    message_type: str
    topic_name: Optional[str] = None
    description: Optional[str] = None
    handler_function: str
    run_as_subprocess: Optional[bool] = Field(default=False)
    is_active: Optional[bool] = Field(default=True)
    prevent_duplication: Optional[bool] = Field(default=True)
    inserted_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None
    keep_log: Optional[bool] = Field(default=False)


class PgBusHostSettings(BaseSettings):
    retry_count: Optional[int] = Field(default=3)
    retry_wait_time: Optional[float] = Field(default=2)
    timeout_in_minutes: Optional[int] = Field(default=1)
    handler_object_cached_duration: Optional[int] = Field(default=0)

    model_config = SettingsConfigDict(env_file_encoding='utf-8',
                                      env_prefix="",
                                      case_sensitive=False)


