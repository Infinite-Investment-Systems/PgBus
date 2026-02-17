import logging
import asyncio
import os
from dotenv import load_dotenv
from infsystems.pgbus.PgBus import PgBus
from infsystems.pgbus.PgBusHandlerContext import PgBusHandlerContext
from infsystems.pgbus.bus_model import *


async def run(context: PgBusHandlerContext, message: PgBusMessage) -> None:
    load_dotenv()

    _logger = logging.getLogger('infinite')
    if not context.bus:
        context.bus = await PgBus.from_connection_string(os.getenv('POSTGRES_URL'))

    if message.message_type == 'sleep':
        _logger.info('Running sleep function.')
        _logger.info(f'About to sleep {message.payload} seconds @{message.queue_name}')
        await asyncio.sleep(int(message.payload))
        _logger.info(f'Just woke up @{message.queue_name}')
    elif message.message_type == 'defer':
        _logger.info('Running defer function.')
        await context.bus.defer(message, int(message.payload))
    elif message.message_type == 'print':
        _logger.info('Running print function.')
        _logger.info(f'"{message.payload}" was sent to {message.queue_name}')
    elif message.message_type == 'raise':
        _logger.info('Running raise function.')
        _logger.info(f'{"abcdefghijklmnopqrstuvwxyz"*100}')
        raise Exception(message.payload)
