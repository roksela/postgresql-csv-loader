import csv
import re
import os
from psycopg2 import connect


class CsvLoader(object):
    """
    Automatically create tables and load data from CSV files to your database.

    This loader creates tables based on CSV headers. Then data is loaded using COPY command.
    It works only with PostgreSQL for now.
    """

    DEFAULT_DELIMITER = ','
    DEFAULT_QUOTE_CHAR = '"'
    DEFAULT_ESCAPE_CHAR = '"'
    DEFAULT_TABLE_PREFIX = "csv_"
    DEFAULT_DATA_TYPE = "varchar"

    CREATE_STMT = "CREATE TABLE {} ({});"
    COPY_STMT = "COPY {} ({}) FROM stdin WITH CSV HEADER DELIMITER '{}' QUOTE '{}' ESCAPE '{}'"

    def __init__(self, database_host, database_port, database_name, user, password=None,
                 table_prefix=DEFAULT_TABLE_PREFIX):
        """
        Constructs document with given database details.

        :param database_host: database host address
        :param database_port: connection port number
        :param database_name: the database name
        :param user: user name used to authenticate
        :param password: password used to authenticate
        :param table_prefix: prefix for database tables that will be created by loader
        """
        self._database_host = database_host
        self._database_port = database_port
        self._database_name = database_name
        self._user = user
        self._password = password
        self._table_prefix = table_prefix

    def load_data(self, file_path, delimiter=DEFAULT_DELIMITER, quote_char=DEFAULT_QUOTE_CHAR,
                  escape_char=DEFAULT_ESCAPE_CHAR, create_table=True):
        """
        Loads data from CSV file to the database.

        Table column names are based on CSV header and names are simplified:
        - all uppercase letters are replaced with underscore and lowercase letters.
        - special characters are replaced with underscore.
        Table name is specified based on CSV file name.

        :param file_path: path to a CSV file
        :param delimiter: a one-character string used to separate fields. It defaults to ','
        :param quote_char: a one-character string used to quote fields containing special characters,
        such as the delimiter or quotechar, or which contain new-line characters
        :param escape_char: a one-character string used by the writer to escape the delimiter
        :param create_table: if True, table will be created
        """
        original_headers = self._read_headers(file_path, delimiter, quote_char, escape_char)
        headers = self._normalize_headers(original_headers)
        table_name = self._generate_table_name(file_path)

        connection = connect(dbname=self._database_name, user=self._user, password= self._password,
                             host=self._database_host, port=self._database_port)
        if create_table:
            self._create_table(connection, headers, table_name)
        self._copy_from_csv(connection, file_path, table_name, headers, delimiter, quote_char, escape_char)

        connection.close()

    def _read_headers(self, file_path, delimiter=DEFAULT_DELIMITER, quote_char=DEFAULT_QUOTE_CHAR,
                  escape_char=DEFAULT_ESCAPE_CHAR):
        """
        Reads CSV header and provides a list of columns.

        :param file_path: path to a CSV file
        :param delimiter: a one-character string used to separate fields. It defaults to ','
        :param quote_char: a one-character string used to quote fields containing special characters,
        such as the delimiter or quotechar, or which contain new-line characters
        :param escape_char: a one-character string used by the writer to escape the delimiter
        :return: list of CSV columns
        """
        with open(file_path, "r") as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quote_char, escapechar=escape_char)
            original_headers = next(reader)
        return original_headers

    def _normalize_headers(self, original_headers):
        """
        Simplifies column names:
        - all uppercase letters are replaced with underscore and lowercase letters.
        - special characters are replaced with underscore.

        :param original_headers: list of CSV columns
        :return: simplified headers
        """
        headers = [self._simplify_text(header) for header in original_headers]
        return headers

    def _generate_table_name(self, file_path):
        """
        Generates table name based on CSV file name.

        :param file_path: path to a CSV file
        :return: generated table name
        """
        base = os.path.splitext(os.path.basename(file_path))[0]
        return self._table_prefix + CsvLoader._simplify_text(base)

    def _create_table(self, connection, headers, table_name):
        """
        Creates database table.

        :param connection: open connection
        :param headers: a list of columns
        :param table_name: a table name
        """
        columns = ['"{}" {}'.format(column, self.DEFAULT_DATA_TYPE) for column in headers]
        columns_def = ",".join(columns)

        cursor = connection.cursor()
        cursor.execute(self.CREATE_STMT.format(table_name, columns_def))
        connection.commit()
        cursor.close()

    def _copy_from_csv(self, connection, file_path, table_name, headers, delimiter, quote_char,
                  escape_char):
        """
        Copies data from CSV to database.

        :param connection: open connection
        :param file_path: path to a CSV file
        :param table_name: a table name
        :param headers: a list of columns
        :param delimiter: a one-character string used to separate fields. It defaults to ','
        :param quote_char: a one-character string used to quote fields containing special characters,
        such as the delimiter or quotechar, or which contain new-line characters
        :param escape_char: a one-character string used by the writer to escape the delimiter
        """
        columns = ['"{}"'.format(column) for column in headers]
        columns_def = ",".join(columns)
        command = self.COPY_STMT.format(table_name, columns_def, delimiter,
                                        quote_char, escape_char)
        # https://www.postgresql.org/docs/current/static/sql-copy.html

        cursor = connection.cursor()
        with open(file_path, "r") as csv_file:
            # cursor.copy_from(csv_file, table_name, columns=headers, sep=delimiter)
            cursor.copy_expert(command, csv_file)
            connection.commit()

    def _create_index(self, original_header):
        # TODO: implement indexing
        pass

    @staticmethod
    def _simplify_text(text):
        """
        Simplifies text:
        - all uppercase letters are replaced with underscore and lowercase letters.
        - special characters are replaced with underscore.

        :param text: text to simplify
        :return: simplified text
        """
        # replace <letter in uppercase> with <letter in lowercase prefixed by underscore>)
        # e.g. SimpleText -> _simple_text
        unified = re.sub('([A-Z]{1})', r'_\1', text).lower()
        # replace all special characters with underscore
        unified = re.sub('[^0-9a-zA-Z]+', '_', unified)

        # remove underscore at the beginning
        if unified.startswith("_"):
            unified = unified[1:]
        return unified
