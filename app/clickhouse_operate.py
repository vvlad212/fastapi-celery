import logging
import backoff
from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, CannotParseUuidError, Error
from core import *

logger = logging.getLogger(__name__)


class ClickHouse:
    def __init__(self):
        self.clickhouse_host = CLICKHOUSE_HOST
        self.clickhouse_user = CLICKHOUSE_USER
        self.clickhouse_password = CLICKHOUSE_PASSWORD
        self.clickhouse_database = CLICKHOUSE_DATABASE
        self.clickhouse_table = CLICKHOUSE_TABLE

    @backoff.on_exception(
        backoff.fibo,
        exception=(ConnectionRefusedError, Error),
        max_time=60,
        max_tries=100,
    )
    def connection(self):
        client = Client(host=self.clickhouse_host,
                        user=self.clickhouse_user,
                        password=self.clickhouse_password)
        try:
            logger.info('Trying to connect clickhouse.')
            client.connection.connect()
            logger.info('Connected to clickhouse.')
            return client
        except Exception as ex:
            logger.error(f'Connection refused error clickhouse. {ex}')
            raise ConnectionRefusedError

    @backoff.on_exception(
        backoff.fibo,
        exception=(NetworkError, CannotParseUuidError),
        max_time=60,
    )
    def ch_insert(self, insert_values: list):
        try:
            client = self.connection()
            logger.info(f'Start insert in clickhouse.')
            client.execute(
                f"INSERT INTO {self.clickhouse_database}.{self.clickhouse_table} (film_id, user_id, timestamp) VALUES",
                (tuple(row) for row in insert_values))
            logger.info(f'{len(insert_values)} row(s) added in clickhouse.')
            client.disconnect()
        except NetworkError as ex:
            logger.error(f'Query error CLICKHOUSE {ex}')
            raise ConnectionRefusedError
        except Exception as ex:
            logger.error(f'Error when pasting data on clickhouse. {ex}')
