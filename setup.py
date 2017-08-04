"""
    Automatically create tables and load data from CSV files to your database.

    This loader creates tables based on CSV headers. Then data is loaded using COPY command.
    It works only with PostgreSQL for now.
"""

from setuptools import setup, find_packages

NAME = "postgresql_csv_loader"
VERSION = "1.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["psycopg2"]

setup(
    name=NAME,
    version=VERSION,
    description="Automatically create tables and load data from CSV files to your database. Python + PostgreSQL.",
    author_email="",
    url="https://github.com/roksela/postgresql-csv-loader",
    keywords=["python", "postgresql", "csv", "loader", "schema-generation"],
    install_requires=REQUIRES,
    packages=find_packages(),
    include_package_data=True,
    long_description="""\
    Automatically create tables and load data from CSV files to your database.\
    This loader creates tables based on CSV headers. Then data is loaded using COPY command.\
    It works only with PostgreSQL for now.\
    """
)