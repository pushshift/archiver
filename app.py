#!/usr/bin/env python3

import falcon
import ujson as json
import gzip
import lzma
import brotli
import os
import psycopg2
import dbconnector

class Testpool():
    def on_get(self, req, resp):
        conn = None
        try:
            conn_failure = False
            conn = dbconnector.pool.getconn()
            cur = conn.cursor()
            cur.execute("SELECT count(*) from source")
        except psycopg2.OperationalError as e:  # Something is FUBAR with the DB so try to reconnect
            conn_failure = True
        else:
            dbconnector.pool.putconn(conn)

        if conn_failure:
            try:
                dbconnector.pool.reconnect()
            except psycopg2.OperationalError as e: # Could't reconnect this time -- return conn to pool and raise error
                pass # We're going to throw an error anyway at this point (this reconnect attempt is for the next incoming connection)
            finally:
                if conn: dbconnector.pool.putconn(conn)
                raise falcon.HTTPServiceUnavailable(title='The database is currently offline. Please try again later.')

        resp.body = json.dumps(cur.fetchall()[0])

class Ingest():
    def on_post(self, req, resp):
        post_data = req.stream.read()
        token = req.get_header('Token',required=True)

        if token != ARCHIVE_TOKEN:
            raise falcon.HTTPUnauthorized(title="Invalid Token")

        content_encoding = req.get_header('Content-Encoding')

        if content_encoding == 'br':
            try:
                post_data = brotli.decompress(post_data)
            except brotli.brotli.Error as e:
                raise falcon.HTTPUnprocessableEntity(title='Payload data compressed with brotli could not be decompressed.',description=e)
        elif content_encoding == 'gzip':
            try:
                post_data = gzip.decompress(post_data)
            except OSError as e:
                raise falcon.HTTPUnprocessableEntity(title='Payload data compressed with gzip could not be decompressed',description=e)
        elif content_encoding == 'lzma':
            try:
                post_data = lzma.decompress(post_data)
            except lzma.LZMAError as e:
                raise falcon.HTTPUnprocessableEntity(title='Payload data compressed with lzma could not be decompressed',description=e)
        elif content_encoding is not None:
            raise falcon.HTTPUnprocessableEntity(title='Payload data could not be decompressed',
                                                        description='Check that you are using the correct Content-Encoding header. The received value was "{}". Accepted compression types are br, gzip and lzma'.format(content_encoding))

        try:
            json_data = json.loads(post_data)
        except Exception as e:
            raise falcon.HTTPUnprocessableEntity(title='Payload data is not valid JSON',description=e)

        # Check if each data element has the required fields
        for obj in json_data:
            print(obj['source'],obj['event'])


#conn = dbconnector.pool.getconn()
#cur = conn.cursor()
#cur.execute("SELECT count(*) from source")
#print(cur.fetchall())

ARCHIVE_TOKEN = os.environ.get('ARCHIVE_TOKEN')
api = falcon.API()
api.add_route('/ingest', Ingest())
api.add_route('/testpool',Testpool())
