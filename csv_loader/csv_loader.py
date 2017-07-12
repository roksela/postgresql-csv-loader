import csv
import re


class CsvLoader(object):
    """
    Automatically create and populate a database using a set of CSV files.
    """

    _default_delimiter = ','
    _default_quotechar = '"'

    def __init__(self, filename=None, delimiter=_default_delimiter, quotechar=_default_quotechar):
        # TODO: documentation
        self.filename = filename
        self.delimiter = delimiter
        self.quotechar = quotechar

        with open(self.filename, "r") as csv_file:
            reader = csv.reader(csv_file, delimiter=self.delimiter, quotechar=self.quotechar)
            self._original_headers = next(reader)

        self._headers = self._unify_headers()

    @property
    def original_headers(self):
        return self._original_headers

    @property
    def headers(self):
        return self._headers

    def _unify_headers(self):
        # headers = [header.lower() for header in self._original_headers]
        headers = [self._unify_header(header) for header in self._original_headers]
        return headers

    @staticmethod
    def _unify_header(header):
        # replace <letter in uppercase> with (_ + <letter in lowercase>)
        unified = re.sub('([A-Z]{1})', r'_\1', header).lower()

        # remove underscore at the beginning
        if unified.startswith("_"):
            unified = unified[1:]
        return unified
