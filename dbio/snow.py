#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

import snowflake.connector

from dbio.base import DBConnection
from dbio.utils import get_config


class SnowflakeConnection(DBConnection):

    def _connect(self):
        user = get_config(self.connection_name, 'user')
        password = get_config(self.connection_name, 'password')
        account = get_config(self.connection_name, 'account')
        warehouse = get_config(self.connection_name, 'warehouse')
        role = get_config(self.connection_name, 'role')

        return snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            role=role,
        )

    def write_csv(self, file_path, table, schema):
        with self.cursor() as cursor:
            filename = os.path.basename(file_path)
            cursor.execute('use database {};'.format(schema))
            cursor.execute('create or replace stage etl_stage;')
            cursor.execute('put file:///{} @etl_stage/{};'.format(file_path, filename))
            cursor.execute('copy into {} from @etl_stage/{}'
                           ' file_format=(type=csv field_optionally_enclosed_by=\'"\')'
                           ' purge=true;'.format(table, filename))

    def read(self, schema, query):
        with self.cursor() as cursor:
            cursor.execute('use database {}'.format(schema))
            cursor.execute(query)
            fields = map(lambda x: x[0], cursor.description)
            return [dict(zip(fields, row)) for row in cursor.fetchall()]
