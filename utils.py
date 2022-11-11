import logging
import shutil
from sqlalchemy import create_engine

LOGGER = logging.getLogger(__name__)


def get_connection_engine(config):
    """
    Build a connection engine for executing sql statements via SQLAlchemy
    :param config:
    :return: engine (sqlalchemy object)
    """
    db_uri = f"mysql+pymysql://{config['db_user']}:{config['db_password']}@" \
             f"{config['db_host']}:{config['db_port']}/{config['db_name']}"
    engine = create_engine(db_uri, echo=config['log_sql_statements'])
    return engine


def move_file(file_path, directory, message=''):
    if message:
        LOGGER.info(f"Moving {file_path} to {directory}. Message: {message}")
    else:
        LOGGER.info(f"Moving {file_path} to {directory}.")
    shutil.move(file_path, directory)


# def get_primary_keys(conn, table_name):
#     """
#     Get primary keys associated with a table
#
#     :param conn: Connection object for pandas
#     :param table_name: table name that we are checking for primary keys
#     :return: Dataframe of primary keys
#     """
#     existing_pk = read_sql('SELECT k.COLUMN_NAME '
#                            'FROM information_schema.table_constraints t '
#                            'LEFT JOIN information_schema.key_column_usage k '
#                            'USING(constraint_name, table_schema, table_name)'
#                            'WHERE t.constraint_type = "PRIMARY KEY" '
#                            f'AND table_name = "{table_name}"'
#                            , conn)
#     return existing_pk