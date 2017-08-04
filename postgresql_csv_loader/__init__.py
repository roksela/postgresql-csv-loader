"""
    Automatically create tables and load data from CSV files to your database.

    This loader creates tables based on CSV headers. Then data is loaded using COPY command.
    It works only with PostgreSQL for now.
"""

from .csv_loader import CsvLoader
