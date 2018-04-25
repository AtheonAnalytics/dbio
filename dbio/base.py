#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
import os
import tempfile
from contextlib import contextmanager

import pandas as pd
import yaml

import dbio

def get_config(connection_name, key):
    config_file = os.environ.get('DBIO_CONFIG_FILE', '~/.dbio_conf')
    with open(config_file, 'r') as f:
        data = yaml.load(f)
    try:
        data = data['dbio-conf']
    except KeyError as e:
        raise MisconfigurationException('"dbio-conf" key not found in config file.')

    try:
        conn_conf = data[connection_name]
    except KeyError as e:
        raise MisconfigurationException('Connection name not found in config file.')

    try:
        return conn_conf[key]
    except KeyError as e:
        raise MisconfigurationException('Value of "{}" not found in config.'.format(key))


def db_connection(connection_name):
    conn_type = get_config(connection_name, 'type')
    type_lookup = {
        'exasol': dbio.ExasolConnection,
        'snowflake': dbio.SnowflakeConnection,
    }
    return type_lookup[conn_type](connection_name)


class DBConnection(object):

    def __init__(self, connection_name):

        self.connection_name = connection_name

    def _connect(self, *args, **kwargs):
        raise NotImplementedError

    def write_csv(self, file_path, table, schema):
        raise NotImplementedError

    def write_pandas(self, data_frame, table, schema):

        with tempfile.NamedTemporaryFile() as temp_file:
            data_frame.to_csv(temp_file, index=False, header=None, quoting=csv.QUOTE_MINIMAL)
            temp_file.flush()
            self.write_csv(temp_file.name, table, schema)

    def read(self, schema, query):
        raise NotImplementedError

    def read_pandas(self, *args, **kwargs):
        return pd.DataFrame(self.read(*args, **kwargs))

    @contextmanager
    def connection(self):
        conn = self._connect()
        yield conn
        conn.commit()
        conn.close()

    @contextmanager
    def cursor(self):
        """
        Context manager to give access to db cursor,
        automatically closing connection after and committing changes.

        """
        with self.connection() as conn:
            cursor = conn.cursor()
            yield cursor


class MisconfigurationException(Exception):
    pass
