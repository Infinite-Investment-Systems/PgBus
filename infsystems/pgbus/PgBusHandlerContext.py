from typing import Any
from infsystems.pgbus.bus_model import PgBusHostSettings
from infsystems.pgbus.BaseBus import BasePgBus
import asyncpg

class PgBusHandlerContext:
    """
    An instance of this class will be created and sent to messaging processing functions, so that they can
    access the same pg connection and same bus instance, in case of in-proc handlers.
    for processing functions that needs to be run in a new process (run_as_subprocess), since we can't
    serialize those objects, new instances will be created. In this case, you can't rely on transaction consistency.
    """

    def __init__(self,
                 bus: BasePgBus|None = None,
                 setting: PgBusHostSettings|None = None,
                 shared_pg_connection: asyncpg.connection.Connection|None = None,
                 context_object_in_proc: Any|None = None,
                 container: Any|None = None):
        self.bus: BasePgBus|None = bus
        self.setting: PgBusHostSettings|None = setting
        self.shared_pg_connection = shared_pg_connection
        self.context_object_in_proc = context_object_in_proc
        self.container: Any|None = container
