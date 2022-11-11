import pymysql
import re
import sqlalchemy.types
import shutil
from pandas import read_sql
from sqlalchemy import create_engine


def get_connection_engine(config, verbose=False):
    """
    Build a connection engine for executing sql statements via SQLAlchemy

    :param config:
    :param verbose: if true, logs all sql statements that are executed by the connection
    :return: engine (sqlalchemy object)
    """
    db_uri = f"mysql+pymysql://{config['db_user']}:{config['db_password']}@" \
             f"{config['db_host']}:{config['db_port']}/{config['db_name']}"
    engine = create_engine(db_uri, echo=verbose)
    return engine


def get_data_type_classes(data_types):
    """
    Since the strings for column data types cannot be passed to the to_sql dtypes argument,
    change each string to a sqlalchemy.types class reference and pass any args it includes.

    :param data_types: dict with column headers as keys, and column data types as string values
    :return updated_data_types: values transformed to class references
    """
    updated_data_types = dict()
    for key, dt in data_types.items():
        if "(" in dt:
            ref, num = re.match(r"(.+)\((.+)\)", dt).group(1, 2)  # Use regex to grab the class reference and the arguments, and remove the parenthesis
            if num.isdigit():
                num = int(num)
            ref = getattr(sqlalchemy.types, ref)(num)
            print(ref)
        else:
            ref = getattr(sqlalchemy.types, dt)
            updated_data_types[key] = ref
    return updated_data_types


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