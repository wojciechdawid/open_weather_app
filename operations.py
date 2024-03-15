from sqlalchemy import Engine, create_engine, CursorResult
from sqlalchemy.sql import text
from geoalchemy2 import Geometry

from config.api_config import DB_CONFIG


class DbConnector:

    @staticmethod
    def connect_db(db_name: str = DB_CONFIG['db_name']['weather'], echo: bool = False) -> Engine:
        return create_engine(f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{db_name}", echo=echo)

    @staticmethod
    def query_db(query: str) -> CursorResult:
        db_conn = DbConnector.connect_db()
        with db_conn.connect() as conn:
            statement = text(query)
            return conn.execute(statement)
