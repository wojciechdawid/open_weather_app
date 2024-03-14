from sqlalchemy import Engine, create_engine
from geoalchemy2 import Geometry


class DbConnector:

    @classmethod
    def connect_db(db_name: str, echo: bool = False) -> Engine:
        return create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}", echo=echo)

