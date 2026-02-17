import multiprocessing
import logging.handlers
import platform
import traceback
# noinspection PyProtectedMember
from pydoc import locate
from infsystems.pgbus.PgBusHandlerContext import *
import asyncio
from dotenv import load_dotenv
from infsystems.pgbus.bus_model import PgBusMessage


class NoRetryException(Exception):
    """
    Custom exception class to signal the consumer not to attempt any retries
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class PgBusSubProcessFork:
    """
    A utility class to help with forking a new process and running the processing functions as a separate
    python process. This is only used for queues that are registered with run_as_subprocess as True.
    The parent process will call the child process asynchronously as a coroutine.
    Log entries emitted by the child process are exchanged back to the parent process to log.
    Exceptions thrown by the child process are also exchanged back to the parent process to catch and handle.
    """
    async def run(self,
                  context: PgBusHandlerContext,
                  message: PgBusMessage,
                  handler_function: str
                  ):
        with multiprocessing.Manager() as mp_manager:
            logger_queue = mp_manager.Queue()
            exception_queue = mp_manager.Queue()

            handlers = self._get_all_configured_handlers()
            listener = logging.handlers.QueueListener(
                logger_queue,
                *handlers,
                respect_handler_level=True)
            listener.start()

            process = multiprocessing.Process(target=self._process_message,
                                              args=(context,
                                                    message,
                                                    handler_function,
                                                    logger_queue,
                                                    exception_queue))
            process.name = f'{message.queue_name}-{message.message_type}-{message.key}'
            process.start()

            # Wait for the process to complete using an executor to avoid blocking the asyncio event loop
            await asyncio.get_event_loop().run_in_executor(None, process.join)
            listener.stop()

            exception = None
            try:
                exception = exception_queue.get(timeout=1)
            except (Exception,):
                pass
            finally:
                self._deplete_queue(exception_queue)
                self._deplete_queue(logger_queue)
                try:
                    process.terminate()
                    if process.is_alive():
                        process.kill()
                except Exception as ex:
                    print(ex)

            if exception:
                raise exception

    @staticmethod
    def _deplete_queue(queue: multiprocessing.Queue):
        while True:
            try:
                queue.get(True, 0.1)
            except Exception as ex:
                print(ex)
                # try:
                #     queue.cancel_join_thread()
                # except Exception as ex2:
                #     print(ex2)
                #     pass
                return

    @staticmethod
    def _process_message(context: PgBusHandlerContext,
                         message: PgBusMessage,
                         handler_function: str,
                         logger_queue: multiprocessing.Queue,
                         exception_queue: multiprocessing.Queue):
        load_dotenv()
        _logger = logging.getLogger('infinite.PgBus')
        _logger.info(f'Running {handler_function} in a sub-process....')
        try:
            h = logging.handlers.QueueHandler(logger_queue)
            root = logging.getLogger()
            root.addHandler(h)
            root.setLevel(logging.INFO)
            processor = locate(handler_function)
            # noinspection PyCallingNonCallable
            if platform.system() == 'Windows':
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            # noinspection PyCallingNonCallable
            asyncio.run(main=processor(context=context, message=message))
            _logger.info(f'{handler_function} finished successfully....')

        except Exception as ex:
            if isinstance(ex, NoRetryException):
                exception_queue.put(NoRetryException(traceback.format_exc()))
            else:
                exception_queue.put(Exception(traceback.format_exc()))
            raise

    @staticmethod
    def _get_all_configured_handlers():
        """Retrieve all handlers from the configured loggers, including the root logger."""
        all_handlers = []
        # Retrieve handlers from the root logger
        for handler in logging.getLogger().handlers:
            all_handlers.append(handler)

        # TODO: needs a bit more investigation
        # Retrieve handlers from all other configured loggers
        # for logger_name in logging.Logger.manager.loggerDict:
        #     logger = logging.getLogger(logger_name)
        #     if hasattr(logger, 'handlers'):
        #         all_handlers.extend(logger.handlers)
        return all_handlers
