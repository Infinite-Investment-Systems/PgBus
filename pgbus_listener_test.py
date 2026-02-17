import logging
import asyncio
import platform
import coloredlogs
import os
from dotenv import load_dotenv
from infsystems.pgbus.PgBus import PgBus
from infsystems.pgbus.PgBusHost import PgBusHost
from infsystems.pgbus.bus_model import *


async def main():
    load_dotenv()
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    _logger = logging.getLogger('infinite')
    _logger.setLevel(logging.INFO)
    coloredlogs.install(level='DEBUG', logger=_logger)
    conn_str = os.getenv('POSTGRES_URL')
    pg_bus = await PgBus.from_connection_string(conn_str)
    settings = PgBusHostSettings(retry_count=3, retry_wait_time=3, max_workers_per_queue=3)
    bus_host = PgBusHost(
        bus=pg_bus,
        connection_string_bypassed_pgbouncer=conn_str,
        setting=settings,
        logger=_logger)

    await bus_host.start(['test']) #, 'test2', 'test3'])

if __name__ == '__main__':

    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
