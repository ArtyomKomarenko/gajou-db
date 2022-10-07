import logging
import os

from dotenv import load_dotenv
from pypika import Query, Table

from src.gajou_db.db_helper import PostgresHelper
from src.gajou_db.singleton import Singleton

load_dotenv()


class ParametrizedDB:
    __metaclass__ = Singleton

    def __init__(self, host, port, user, password, dbname):
        self.connection = PostgresHelper(host, port, user, password, dbname)

    def get_users(self):
        users = Table('users_user')
        query = Query.from_(users).select(users.star).limit(100)
        return self.connection.select_all(query, use_cache=True)


class DsnDB:
    __metaclass__ = Singleton

    def __init__(self, dsn):
        self.connection = PostgresHelper(dsn=dsn)

    def get_users(self):
        users = Table('users_user')
        query = Query.from_(users).select(users.star).limit(100)
        return self.connection.select_all(query, use_cache=True)


if __name__ == '__main__':
    # logger configuration to demonstrate "out-of-the-box" logs
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt='[%(levelname)s] %(asctime)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    dbname = os.getenv('POSTGRES_DBNAME')

    # Creating connection using each parameter separately
    p_db = ParametrizedDB(host, port, user, password, dbname)
    p_db.get_users()

    # Creating connection using DSN sting
    d_db = DsnDB(dsn=f'host={host} port={port} user={user} password={password} dbname={dbname} sslmode=disable')
    d_db.get_users()

    # Creating connection as context
    with PostgresHelper(host, port, user, password, dbname) as db_connect:
        users = Table('users')
        query = Query.from_(users).select(users.star).limit(100)
        db_connect.select_one(query)
