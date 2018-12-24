#!/usr/bin/env python3

import falcon
import ujson as json
import gzip
import lzma
import brotli
import os
import psycopg2
import dbconnector
import helpers

# Eventually all the try / except catching on the DB pool connection objects should be handled via extending / subclassing
# the cursor object: http://initd.org/psycopg/docs/advanced.html
#
# The following code words for now but can be optimized later (right)
#
#

class Testpool():
    def on_get(self, req, resp):

        conn = dbconnector.pool.getconn()

        try:
            conn_failure = False
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


class Ingest():
    def on_post(self, req, resp):
        post_data = req.stream.read()
        print(len(post_data))
        #resp.body = json.dumps(['a','b'])
        auth_type, token = req.get_header('Authorization',required=True).split()

        if token != ARCHIVE_TOKEN or auth_type.lower() != 'bearer':
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

#        resp.body = json.dumps(['a','b'])

        db_data = []

        for obj in json_data:
            source = obj.get('source')
            entity = obj.get('entity')
            id = obj.get('id')
            retrieved_utc = obj.get('retrieved_utc')
            if "{}-{}".format(source,entity) not in thing_ids:
                thing_ids.update(helpers.get_thing_id(dbconnector.pool.getconn(),source,entity))
            thing_id = thing_ids["{}-{}".format(source,entity)]
            db_data.append((thing_id,id,retrieved_utc,json.dumps(obj['data']).encode()))

        if db_data:
            conn = dbconnector.pool.getconn()
            cur = conn.cursor()
            sql = "INSERT INTO archive (thing_id,item_id,retrieved_utc,data) VALUES {} ON CONFLICT (thing_id,item_id) DO NOTHING"
            args_str = b','.join(cur.mogrify("(%s,%s,%s,%s)", x) for x in db_data)
            sql = sql.format(args_str.decode("utf-8"))
            cur.execute(sql)
            conn.commit()
            dbconnector.pool.putconn(conn)


ARCHIVE_TOKEN = os.environ.get('ARCHIVE_TOKEN')
conn = dbconnector.pool.getconn()
#print (conn.encoding)
thing_ids = {}
thing_ids = helpers.get_all_things(conn)
#thing_id = helpers.get_thing_id(conn,'a','b')
#print(thing_ids)
api = falcon.API()
api.add_route('/ingest', Ingest())
api.add_route('/testpool',Testpool())
