#!/usr/bin/python
# -*- coding: utf-8 -*-
import snowflake.connector


from dbio.base import DBConnection, MisconfigurationException, get_config


class SnowflakeConnection(DBConnection):

    def _connect(self):
        try:
            user = get_config(self.connection_name, 'user')
            password = get_config(self.connection_name, 'password')
            account = get_config(self.connection_name, 'account')
            warehouse = get_config(self.connection_name, 'warehouse')
            role = get_config(self.connection_name, 'role')
        except KeyError as e:
            raise MisconfigurationException(str(e))

        return snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            role=role,
        )

    def write_csv(self, file_path, table, schema):

        with self.cursor() as cursor:
            cursor.execute('use database {}'.format(schema))
            cursor.execute('put file:///{} @%{};'.format(file_path, table))
            cursor.execute('copy into {} from @%{}'
                           ' file_format=(type=csv field_optionally_enclosed_by=\'"\')'
                           ' purge=true;'.format(table, table))

    def read(self, schema, query):
        with self.cursor() as cursor:
            cursor.execute('use database {}'.format(schema))
            results = cursor.execute(query)
            return results.fetchall()


