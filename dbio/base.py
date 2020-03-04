#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
import tempfile
from contextlib import contextmanager

import pandas as pd


class DBConnection(object):
    """
    Base db connection.

    """

    def __init__(self, connection_name):
        self.connection_name = connection_name

    def _connect(self):
        """
        Abstract method to connect to databases.

        Returns:
            Database connection object

        """
        raise NotImplementedError

    def write_csv(self, file_path, table, schema, columns=None):
        """
        Abstract method to write csv to database.

        Args:
            file_path(str): path to file
            table(str): table in db
            schema(str): schema in db

        """
        raise NotImplementedError

    def write_pandas(self, data_frame, table, schema):
        """
        Write pandas dataframe to database.

        Utilises write_csv method, but can be overwritten if db technology offers more efficient way.

        Args:
            data_frame(pandas dataframe): dataframe to write.
            table(str): table in db
            schema(str): schema in db

        """
        columns = data_frame.columns.tolist()
        with tempfile.NamedTemporaryFile(mode='w') as temp_file:
            data_frame.to_csv(temp_file, index=False, header=None, quoting=csv.QUOTE_MINIMAL, encoding='utf8')
            temp_file.flush()
            self.write_csv(temp_file.name, table, schema, columns=columns)

    def read(self, schema, query):
        """
        Abstract method to read from database.

        Args:
            schema(str): schema in db
            query(str): db query


        Returns:
            list

        """
        raise NotImplementedError

    def read_pandas(self, *args, **kwargs):
        """
        Make query and return as pandas dataframe.

        Takes same arguments as self.read.

        Returns:
            pandas dataframe

        """
        return pd.DataFrame(self.read(*args, **kwargs))

    @contextmanager
    def connection(self):
        """
        Connection context manager.

        Closes and commits and exit.

        Returns:
            connection obj

        """
        conn = self._connect()
        yield conn
        conn.commit()
        conn.close()

    @contextmanager
    def cursor(self):
        """
        Context manager to give access to db cursor,
        automatically closing connection after and committing changes.

        Returns:
            cursor obj

        """
        with self.connection() as conn:
            cursor = conn.cursor()
            yield cursor
