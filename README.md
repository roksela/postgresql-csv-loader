# postgresql-csv-loader
Automatically create tables and load data from CSV files to your database.

This loader creates tables based on CSV headers. Then data is loaded using COPY command.
It works only with PostgreSQL for now.

## Installation

```sh
pip3 install git+https://github.com/roksela/postgresql-csv-loader.git
```

## Getting Started

```python
from postgresql_csv_loader import CsvLoader

loader = CsvLoader("host", 5432, "db_name", "user", "password")
loader.load_data("stats.csv")
loader.load_data("departments.csv")
loader.load_data("employees.csv")

```

## Logging

```python
import logging
import sys
from postgresql_csv_loader import CsvLoader

log_format = '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO, stream=sys.stdout)

loader = CsvLoader("host", 5432, "db_name", "user", "password")
loader.load_data("stats.csv")

```

## Character encoding

```python
from postgresql_csv_loader import CsvLoader

loader = CsvLoader("host", 5432, "db_name", "user", "password")
loader.load_data("stats.csv", encoding='iso-8859-2')

```

## Custom format

```python
from postgresql_csv_loader import CsvLoader

loader = CsvLoader("host", 5432, "db_name", "user", "password")
loader.load_data("stats.csv", delimiter=';', quote_char='/', escape_char='\\')

```

## Dependencies

```shell
pip3 install psycopg2
```

## Author

Kris Roksela kris@dataservices.pro
