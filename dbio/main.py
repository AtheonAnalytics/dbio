#!/usr/bin/python
# -*- coding: utf-8 -*-
from .snow import SnowflakeConnection
from .utils import get_config


def db_connection(connection_name):
    """
    Get db connection.

    Args:
        connection_name(str): Connection name, as defined in config file.

    Returns:
        DBConnection

    """
    conn_type = get_config(connection_name, 'type')
    type_lookup = {
        'snowflake': SnowflakeConnection,
    }
    return type_lookup[conn_type](connection_name)
