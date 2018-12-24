#!/usr/bin/env python3

import psycopg2
from psycopg2 import pool
import sys
import os
import configparser
import falcon

class ProcessSafePoolManager:

    def __init__(self,*args,**kwargs):
        self.last_seen_process_id = os.getpid()
        self.args = args
        self.kwargs = kwargs
        self._init()

    def _init(self):
        self._pool = psycopg2.pool.ThreadedConnectionPool(*self.args,**self.kwargs)

    def reconnect(self):
        self._pool = psycopg2.pool.ThreadedConnectionPool(*self.args,**self.kwargs)

    def getconn(self):
        current_pid = os.getpid()
        if not (current_pid == self.last_seen_process_id):
            self._init()
            print ("New id is {}, old id was {}".format(current_pid,self.last_seen_process_id))
            self.last_seen_process_id = current_pid
        try:
            return self._pool.getconn()
        except (psycopg2.pool.PoolError,psycopg2.OperationalError):
            raise falcon.HTTPServiceUnavailable(title='The database currently has no available connections. Please try again.')

    def putconn(self, conn):
        return self._pool.putconn(conn)

config = configparser.ConfigParser()
config.read("credentials.ini")
psql_config = config['psql_database']
pool = ProcessSafePoolManager(25, 100, user = psql_config['user'],
                                                      password = psql_config['password'],
                                                      host = psql_config['host'],
                                                      port = psql_config['port'],
                                                      database = psql_config['dbname'])

