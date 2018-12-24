#!/usr/bin/env python3

import falcon
import dbconnector
import psycopg2


def get_all_things(conn):

    try:
        cur = conn.cursor()
        cur.execute("SELECT id,source,entity FROM thing limit 10000")
    except psycopg2.OperationalError as e:
        conn.rollback()
    else:
        conn.commit()
        data = cur.fetchall()
        things = {}
        for d in data:
            things["{}-{}".format(d[1],d[2])] = d[0]
        return things
    finally:
        dbconnector.pool.putconn(conn)


def get_thing_id(conn,source,entity):
# An thing ID is a unique identifier for an item ingested.
# Example: source=reddit, entity=comment, thing_id=source/entity unique identifer
# The thing id is not the id of the item itself (that is the item ID)

    try:
        do_select = False
        cur = conn.cursor()
        cur.execute("INSERT INTO thing (source,entity) VALUES (%s,%s) returning id",(source.lower(),entity.lower()))
    except psycopg2.IntegrityError:
        conn.rollback()
        do_select = True
    else:
        conn.commit()
        dbconnector.pool.putconn(conn)
        return cur.fetchone()[0]

    if do_select:
        try:
            cur.execute("SELECT id FROM thing WHERE source = %s and entity = %s",(source.lower(),entity.lower()))
            conn.commit()
        except psycopg2.OperationalError as e:
            pass # Do something here later
        else:
            return cur.fetchone()[0]
        finally:
            dbconnector.pool.putconn(conn)
