#!/usr/bin/python
# -*- coding: utf-8 -*-
from collections import OrderedDict

import exasol

from dbio.base import DBConnection
from dbio.utils import get_config


class ExasolConnection(DBConnection):

    def _connect(self):
        return exasol.connect(
            DRIVER=get_config(self.connection_name, 'driver'),
            EXAHOST=get_config(self.connection_name, 'EXAHOST'),
            EXAUID=get_config(self.connection_name, 'EXAUID'),
            EXAPWD=get_config(self.connection_name, 'EXAPWD'),
            ENCRYPTION=get_config(self.connection_name, 'ENCRYPTION'),
            autocommit=True,
            CONNECTIONLCALL='C.UTF-8')

    def write_pandas(self, data_frame, table, schema):
        table_path = '{}.{}'.format(schema, table)
        columns = data_frame.columns.tolist()

        with self.connection() as conn:
            conn.writeData(
                data_frame, table=table_path, chunksize=1000, columnNames=columns, encoding='utf8')

    def read_pandas(self, schema, query):
        with self.connection() as conn:
            conn.execute('open schema {};'.format(schema))
            return conn.readData(sqlCommand=query)

    def read(self, schema, query):
        with self.cursor() as cursor:
            cursor.execute('open schema {};'.format(schema))
            cursor.execute(query)
            fields = map(lambda x: x[0], cursor.description)
            return [OrderedDict(zip(fields, row)) for row in cursor.fetchall()]
