#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

import yaml


def get_config(connection_name, key):
    """
    Get configuration from config file.

    Args:
        connection_name(str): as defined in config.
        key(str): Value required.

    Returns:
        str: config value

    """
    config_file = os.environ.get('DBIO_CONFIG_FILE')
    if not config_file:
        raise MisconfigurationException('DBIO_CONFIG_FILE environment variable not set')

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


class MisconfigurationException(Exception):
    pass
