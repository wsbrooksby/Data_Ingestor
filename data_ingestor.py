# Imports
import sys
import logging.config
import traceback
import os
import pandas as pd
import io
from utils import move_file, get_connection_engine, init_logging, build_default_config
from yaml import safe_load
from epf_file import EPFFile

LOGGER = logging.getLogger(__name__)
CONFIG = dict()


def main():
    init_logging()

    # Import the config file and load it into a dict
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.yaml')
    if os.path.exists(config_path):
        global CONFIG
        CONFIG = safe_load(open(config_path))
    else:
        # If no config file exists, create a blank config file and let the user know to go fill in the required details.
        build_default_config()
        LOGGER.error(f"The required 'config.yaml' file was not located. Please fill the required database credentials into the new file that was created.")
        sys.exit(f"The required 'config.yaml' file was not located. Please fill the required database credentials into the new file that was created.")

    # Create the CONFIG['inboxes'] directories if they don't exist
    for inbox in CONFIG['inboxes'].values():
        os.makedirs(inbox, exist_ok=True)

    # Build list of files being imported
    file_group = build_file_list()
    if not file_group:
        LOGGER.error("No files were successfully loaded for importing")
        sys.exit()
    else:
        LOGGER.info("All EPFFile classes created")

    # Send each file through the import_file script to upload them to the database
    for epf_file in file_group:
        try:
            epf_file = create_dataframe(epf_file, CONFIG['file_structure'])
        except Exception as e:
            LOGGER.error(traceback.format_exc())
            LOGGER.error(f"Unhandled error in import_file: {e}")
            move_file(epf_file.filename, epf_file.file_path, CONFIG['inboxes']["failed_path"], "Unhandled exception")
            continue
        try:
            inserted_record_count = upload_to_database(epf_file)
        except Exception as e:
            LOGGER.error(traceback.format_exc())
            LOGGER.error(f"Unhandled error in import_file: {e}")
            move_file(epf_file.filename, epf_file.file_path, CONFIG['inboxes']["failed_path"], "Unhandled exception")
            continue
        if inserted_record_count:
            move_file(epf_file.filename, epf_file.file_path, CONFIG['inboxes']["finished_path"], f"Successfully imported {inserted_record_count} records from {epf_file.file_path} into database.")
        else:
            move_file(epf_file.filename, epf_file.file_path, CONFIG['inboxes']["failed_path"], f"No records were found in {epf_file.file_path} to import.")
    LOGGER.info("Finished processing all files.")


def build_file_list():
    """
    Builds a list of files in the "ready" path. Each file is built as a class, and attempts to parse the relevant metadata out of the file header.
    If it cannot build the class, the file is instead moved to the failed directory and excluded from the list
    :return:
    """
    file_group = list()
    for root, _, files in os.walk(CONFIG['inboxes']['ready_path']):
        if not files:
            LOGGER.info("No files found to import, exiting the program.")
            break
        for filename in files:
            file_path = os.path.join(root, filename)
            try:
                file = EPFFile(filename, file_path, CONFIG['file_structure'])
            except ValueError:
                move_file(filename, file_path, CONFIG['inboxes']["failed_path"], "Malformed file header.")
                continue
            except Exception as e:
                LOGGER.error(traceback.format_exc())
                move_file(filename, file_path, CONFIG['inboxes']["failed_path"], "Unknown Exception when building EPFFile class.")
                print(e)
                continue
            file_group.append(file)

    return file_group


def create_dataframe(epf_file, file_structure):
    """
    Create a pandas dataframe from the file, and perform data cleaning.
    (This can be expanded to include more cleaning in the future)
    :param epf_file:
    :param file_structure:
    :return:
    """
    # Remove the "\n" characters from the row delimiters (line terminators), since pandas can't handle two-character line terminators
    with open(epf_file.file_path, mode="r", encoding="utf8") as f:
        cleaned_file = f.read().replace(f"{file_structure['row_delimiter']}\n", file_structure['row_delimiter'])

    # Put the data into a pandas dataframe
    # noinspection PyTypeChecker
    df = pd.read_csv(
        io.StringIO(cleaned_file),
        sep=file_structure['field_delimiter'],
        lineterminator=file_structure['row_delimiter'],
        header=0,
        names=epf_file.column_headers,
        comment=file_structure['comment_char'],
        quoting=3,  # Ignores quotation characters.
        index_col=False,
        on_bad_lines='skip',  # skipping rows with too many columns
        encoding='utf8'
    )
    df = df.dropna()  # Dropping rows with too few columns/null values

    epf_file.cleaned_row_count = len(df.index)
    epf_file.cleaned_data = df

    return epf_file


def upload_to_database(epf_file):
    """
    Upload the dataframe to the database, and assign primary key(s)
        - If export_mode == FULL, replaces the data table with the new values.
        - Otherwise, creates a temp table and updates/inserts into the existing table from the temp one. Drops the temp table afterwards.

    :param epf_file:
    :return:
    """
    LOGGER.info(f"Uploading file: {epf_file.filename}\nMode: {epf_file.export_mode}\nHeaders: {epf_file.column_headers}\nData Types: {epf_file.data_types}\nPrimary Keys: {epf_file.primary_keys}")

    table_name = epf_file.filename
    # Use a temp table when doing incremental updates, so that inserts still work when duplicates exist.
    if epf_file.export_mode != "FULL":
        temp_table_name = table_name + "_temp"
    # If doing a full replace, the main table is dropped and a temp table is not needed.
    else:
        temp_table_name = table_name

    # Connect to database (build SQLAlchemy connection engine)
    engine = get_connection_engine(CONFIG['db_info'])
    with engine.connect() as conn:
        # Upload dataframe to MySQL using SQLAlchemy and Pandas to_sql
        inserted_record_count = epf_file.cleaned_data.to_sql(
            temp_table_name,
            con=conn,
            if_exists='replace',
            index=False,
            chunksize=1000,
            dtype=epf_file.data_types
        )

        # Add primary keys to table that was created.
        pk_string = ", ".join(epf_file.primary_keys)
        conn.execute(f"ALTER TABLE {temp_table_name} ADD PRIMARY KEY ({pk_string})")

        # If doing an incremental update, merge the temp table into the main table
        if epf_file.export_mode != "FULL":
            # In case the primary key is composite, the following generates a dynamic join statement, set statement, and where clause
            join_set = list()
            for pk in epf_file.primary_keys:
                join_set.append(f"{table_name}.{pk} = {temp_table_name}.{pk}")
            join_set = " AND ".join(join_set)  # put all joins together, with " AND " in between them
            where_clause = join_set.replace("=", "!=")  # invert the "=" characters in the join set for use in the WHERE clause
            column_names = ", ".join(epf_file.column_headers)  # Build a string of column names for use in the insert statement

            # Update existing records in the main table
            conn.execute(f"UPDATE {table_name} "
                         f"LEFT JOIN {temp_table_name} ON {join_set} "
                         f"SET {join_set} "
                         f"WHERE {where_clause};")

            # Insert new records into the main table
            conn.execute(f"INSERT INTO {table_name} ({column_names}) "
                         f"SELECT {column_names} FROM {temp_table_name} "
                         f"WHERE ({pk_string}) NOT IN (SELECT {pk_string} FROM {table_name});")

            # Delete the temporary table (since to_sql does not support creating a TEMPORARY table in MySQL)
            conn.execute(f"DROP TABLE {temp_table_name};")

    return inserted_record_count


# Execute main method
if __name__ == '__main__':
    main()
