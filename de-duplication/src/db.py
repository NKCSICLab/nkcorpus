import configparser
import dataclasses

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
import pymongo
from pymongo.database import Database


@dataclasses.dataclass
class DatabaseConfig:
    drivername: str = 'mysql'
    username: str = 'root'
    password: str = ''
    host: str = 'localhost'
    port: int = 3306
    database: str = ''


def get_database_config(config: configparser.ConfigParser) -> DatabaseConfig:
    return DatabaseConfig(
        drivername=config.get('database', 'drivername'),
        username=config.get('database', 'username'),
        password=config.get('database', 'password'),
        host=config.get('database', 'host'),
        port=config.getint('database', 'port'),
        database=config.get('database', 'database'),
    )


def db_connect(conf: DatabaseConfig) -> Engine:
    return create_engine(
        f"{conf.drivername}://{conf.username}:{conf.password}@{conf.host}:{conf.port}/{conf.database}?charset=utf8",
        poolclass=NullPool)


def get_mongo_config(config: configparser.ConfigParser) -> DatabaseConfig:
    return DatabaseConfig(
        username=config.get('mongo_db', 'username'),
        password=config.get('mongo_db', 'password'),
        host=config.get('mongo_db', 'host'),
        port=config.getint('mongo_db', 'port'),
        database=config.get('database', 'database'),
    )


def mongo_connect(conf: DatabaseConfig) -> pymongo.MongoClient:
    conn_str = f"mongodb://{conf.username}:{conf.password}@{conf.host}:{conf.port}/{conf.database}"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    return client

