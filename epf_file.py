import logging
import re
import sqlalchemy.types

LOGGER = logging.getLogger(__name__)


class EPFFile:
    def __init__(self, filename, file_path, file_structure):
        # Metadata
        self.filename = filename
        self.file_path = file_path
        self.column_headers = list()
        self.primary_keys = list()
        self.data_types = dict()
        self.export_mode = ''

        # Data added while processing file (for documentation purposes)
        # self.cleaned_row_count = 0
        # self.cleaned_data = pd.dataframe

        self.parse_metadata(file_structure)

    def parse_metadata(self, file_structure):
        """
        Parse and format the metadata from the top of the file for column headers and datatypes, primary key, and export mode.
            - Performs validation on each row to check for correct comment characters, row labels, and number of columns.
            - Maps the data_types variable to a dictionary containing SQLAlchemy class references (needed when writing to database).
        :param file_structure:
        """
        failed_list = list()  # Keeps track of validation steps. Will be list of 0's if the metadata is valid, but will contain a 1 for every failed validation step.

        with open(self.file_path, mode="r", encoding="utf8") as raw_file:
            # The first 4 lines contain the metadata needed for MySQL,
            file_head = [next(raw_file).replace(file_structure['row_delimiter'] + "\n", "") for x in range(4)]  # Strip out the row delimiter, since these lines split correctly on \n

            # Validate correct comment characters
            for row in file_head:
                if not row.startswith(file_structure['comment_char']):
                    LOGGER.error(f'File "{self.filename}" does not contain proper comment characters in the first 4 rows, and may be malformed.')
                    failed_list.append(1)

            # Get column_headers list
            column_headers = file_head[0].strip(file_structure['comment_char']).split(file_structure['field_delimiter'])  # The column headers row does not contain a label

            # Get data_types dictionary
            data_types, failed_list = self.validate_row_label(file_head[2], "dbTypes", self.filename, failed_list)
            data_types = data_types.split(file_structure['field_delimiter'])

            # Get primary_keys list
            primary_keys, failed_list = self.validate_row_label(file_head[1], "primaryKey", self.filename, failed_list)
            primary_keys = primary_keys.split(file_structure['field_delimiter'])

            # Get export_mode
            export_mode, failed_list = self.validate_row_label(file_head[3], "exportMode", self.filename, failed_list)
            export_mode = export_mode.upper()

        if len(data_types) == len(column_headers):
            data_types = dict(zip(column_headers, data_types))  # create a dictionary mapping the column data types to the headers
            data_types = self.get_data_type_classes(data_types)  # change column data types to class references for sqlalchemy
        else:
            LOGGER.error(f'File "{self.filename}" does not have an equal number of header and dbType columns.')
            failed_list.append(1)

        if 1 in failed_list:
            raise ValueError

        # Populate the Metadata variables
        self.column_headers = column_headers
        self.data_types = data_types
        self.primary_keys = primary_keys
        self.export_mode = export_mode

    @staticmethod
    def validate_row_label(row, row_label, filename, failed_list):
        """
        Removes the label and comment character from the beginning of a row, and returns the cleaned row.
        Fails the record for bad metadata headers if:
            - There are no ":" characters in the row
            - There are multiple ":" characters in the row
            - The row does not start with the proper label
        :param row:
        :param row_label: primaryKey, dbTypes, exportMode
        :param filename:
        :param failed_list: append 1 if failed, 0 if passed
        :return:
        """
        row = row.split(":")  # Characters before the colon are the label, and after the colon are the values
        if len(row) != 2 or row_label.lower() not in row[0].lower():
            LOGGER.error(f'File "{filename}" contains malformed {row_label} row')
            failed_list.append(1)
        else:
            row = row[1]
            failed_list.append(0)
        return row, failed_list

    @staticmethod
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
                updated_data_types[key] = ref
            else:
                ref = getattr(sqlalchemy.types, dt)
                updated_data_types[key] = ref
        return updated_data_types
