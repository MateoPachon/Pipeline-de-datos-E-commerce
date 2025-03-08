from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine.base import Engine

def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Carga los DataFrames en la base de datos SQLite.

    Args:
        data_frames (Dict[str, DataFrame]): Diccionario con los DataFrames a cargar.
        database (Engine): Conexión a la base de datos.
    """
    for table_name, df in data_frames.items():
        df.to_sql(table_name, con=database, if_exists="replace", index=False)

