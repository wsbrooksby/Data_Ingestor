import logging
import shutil
import os
from datetime import datetime
from sqlalchemy import create_engine
from yaml import dump

LOGGER = logging.getLogger(__name__)


def get_connection_engine(db_info):
    """
    Build a connection engine for executing sql statements via SQLAlchemy
    :param db_info:
    :return: engine (sqlalchemy object)
    """
    db_uri = f"mysql+pymysql://{db_info['db_user']}:{db_info['db_password']}@" \
             f"{db_info['db_host']}:{db_info['db_port']}/{db_info['db_name']}?charset=utf8mb4"
    engine = create_engine(db_uri, echo=db_info['log_sql_statements'])
    return engine


def move_file(filename, file_path, directory, message=''):
    if message:
        LOGGER.info(f"Moving {file_path} to {directory}. Message: {message}")
    else:
        LOGGER.info(f"Moving {file_path} to {directory}.")
    curr_dt = datetime.now()
    filename += curr_dt.strftime("_%Y%m%d%H%M%S.%f")  # append the current date and time to the filename to make it unique in the new directory
    new_file_path = os.path.join(directory, filename)  # join the new directory to the filename
    try:
        shutil.move(file_path, new_file_path)
    except Exception as e:
        LOGGER.error(f'Unhandled exception occurred while attempting to move "{file_path}" to "{new_file_path}". Exception: {e}')


def init_logging():
    os.makedirs("logs", exist_ok=True)  # Create logging directory if it does not already exist
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'root': {
            'level': 'INFO',
            'handlers': ['console', 'rotatinglog']
        },
        'formatters': {
            'simple': {
                'format': '%(asctime)s [%(levelname)s]: %(message)s'}
        },
        'handlers': {
            'console': {
                'formatter': 'simple',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',
                'level': 'INFO'
            },
            'rotatinglog': {
                'level': 'INFO',
                'formatter': 'simple',
                'filename': 'logs/data_ingestor.log',
                'when': 'midnight',
                'class': 'logging.handlers.TimedRotatingFileHandler'
            }
        }
    }
    logging.config.dictConfig(logging_config)


def build_default_config():
    config_dict = {
        'db_info': {
            'db_host': '',
            'db_port': '',
            'db_name': '',
            'db_user': '',
            'db_password': '',
            'log_sql_statements': False
        },
        'file_structure': {
            'field_delimiter': '\u0001',
            'row_delimiter': '\u0002',
            'comment_char': '#'
        },
        'inboxes': {
            'ready_path': './inboxes/ready/',
            'finished_path': './inboxes/finished/',
            'failed_path': './inboxes/failed/'
        }
    }
    with open('config.yaml', 'w') as config_file:
        dump(config_dict, config_file)
