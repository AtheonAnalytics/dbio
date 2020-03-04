#!/usr/bin/python
# -*- coding: utf-8 -*-
from collections import OrderedDict

import pyexasol.db2 as E

from dbio.base import DBConnection
from dbio.utils import get_config


class ExasolConnection(DBConnection):

    def _connect(self):
        return E.connect(dsn=get_config(self.connection_name, 'EXAHOST'),
                         user=get_config(self.connection_name, 'EXAUID'),
                         password=get_config(self.connection_name, 'EXAPWD'))

    def write_pandas(self, data_frame, table, schema):
        table_path = (schema, table)
        columns = data_frame.columns.tolist()

        with self.connection() as conn:
            conn.import_from_pandas(
                data_frame, table_path, callback_params={'chunksize': 1000, 'columns': columns,
                                                         'encoding': 'utf-8'})

    def read_pandas(self, schema, query):
        with self.connection() as conn:
            conn.execute('open schema {};'.format(schema))
            return conn.export_to_pandas(query)

    def read(self, schema, query):
        with self.cursor() as cursor:
            cursor.execute('open schema {};'.format(schema))
            cursor.execute(query)
            fields = map(lambda x: x[0], cursor.description)
            return [OrderedDict(zip(fields, row)) for row in cursor.fetchall()]
