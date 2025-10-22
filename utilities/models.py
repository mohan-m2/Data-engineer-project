from sqlalchemy import MetaData, Table
from .database_config import engine

def get_dynamic_table(schema_name: str, table_name: str):
    """
    Dynamically reflect a table from any schema at runtime
    Args:
        schema_name: Database schema name
        table_name: Name of the table to reflect
    Returns:
        SQLAlchemy Table object
    """
    metadata = MetaData(schema=schema_name)
    return Table(
        table_name,
        metadata,
        autoload_with=engine
    )