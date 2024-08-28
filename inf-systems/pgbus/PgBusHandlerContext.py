from ..bus.PgBus import *



class PgBusHandlerContext:
    """
    An instance of this class will be created and sent to messaging processing functions, so that they can
    access the same pg connection and same bus instance, in case of in-proc handlers.
    for processing functions that needs to be run in a new process (run_as_subprocess), since we can't
    serialize those objects, new instances will be created. In this case, you can't rely on transaction consistency.
    """

    def __init__(self,
                 bus: Optional[BasePgBus] = None,
                 setting: Optional[PgBusHostSettings] = None,
                 shared_pg_connection: Optional[asyncpg.connection.Connection] = None,
                 container=None):
        self.bus: Optional[BasePgBus] = bus
        self.setting: Optional[PgBusHostSettings] = setting
        self.shared_pg_connection = shared_pg_connection
        self.container = container
