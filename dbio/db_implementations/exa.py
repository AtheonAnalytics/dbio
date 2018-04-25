#!/usr/bin/python
# -*- coding: utf-8 -*-
import exasol

from dbio.base import DBConnection, get_config


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

    def read(self, schema, query):
        with self.connection() as conn:
            conn.execute('open schema {};'.format(schema))
            return conn.readData(sqlCommand=query)
