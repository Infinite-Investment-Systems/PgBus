import logging
import asyncio
import platform
import os
from dotenv import load_dotenv
from infsystems.pgbus.PgBus import PgBus
from infsystems.pgbus.PgBusHost import PgBusHost
from infsystems.pgbus.bus_model import *


async def main():
    load_dotenv()
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    _logger = logging.getLogger('infinite')
    # coloredlogs.install(level='DEBUG', logger=_logger)
    conn_str = os.getenv('POSTGRES_URL')
    pg_bus = await PgBus.from_connection_string(conn_str)

    bus_host = PgBusHost(
        bus=pg_bus,
        connection_string_bypassed_pgbouncer=conn_str,
        logger=_logger)

    await bus_host.register(PgBusRegistration(
        queue_name='test',
        message_type='print',
        keep_log=True,
        handler_function='TestHandler.run',
        run_as_subprocess=False))

    await bus_host.register(PgBusRegistration(
        queue_name='test',
        message_type='raise',
        keep_log=True,
        handler_function='TestHandler.run',
        run_as_subprocess=False))

    await bus_host.register(PgBusRegistration(
        queue_name='test',
        message_type='sleep',
        handler_function='TestHandler.run',
        keep_log=True,
        run_as_subprocess=False))

    await bus_host.register(PgBusRegistration(
        queue_name='test',
        message_type='defer',
        keep_log=True,
        handler_function='TestHandler.run', run_as_subprocess=False ) )


    for i in range(30):
        await pg_bus.send(queue_name='test', message_type='print', payload=f'message #{i} to be printed by test!')
    #     await pg_bus.send(queue_name='test', message_type='print', payload=f'message #{i} to be printed by test2!')
    #     await pg_bus.send(queue_name='test', message_type='sleep', payload=f'{i*1}')
    #     # await pg_bus.send(queue_name='test2', message_type='asleep', payload=f'{i*2}')
    #     # time.sleep(4)
    # await pg_bus.send(queue_name='test', message_type='defer', payload='1')
    # await pg_bus.send(queue_name='test', message_type='raise', payload='testing failure')
    # # await pg_bus.send(queue_name='test', message_type='sleep', payload='40')
    # await pg_bus.send(queue_name='test', message_type='print', payload='Another message to be printed!')


if __name__ == '__main__':
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
