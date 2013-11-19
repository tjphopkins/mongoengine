import unittest
import pymongo

import mongoengine.connection

from mongoengine import *
from mongoengine.connection import (
    get_db, get_connection, register_db, ConnectionError)


class ConnectionTest(unittest.TestCase):

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_connect(self):
        """Ensure that the connect() method works properly.
        """
        connect()
        register_db('mongoenginetest')

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.connection.Connection))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        connect(alias='testdb')
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.connection.Connection))

    def test_connect_uri(self):
        """Ensure that the connect() method works properly with uri's
        """
        c = connect(alias='admin')
        register_db('mongoenginetest', 'admin', 'admin')
        c.admin.system.users.remove({})
        c.mongoenginetest.system.users.remove({})

        c.admin.add_user("admin", "password")
        c.admin.authenticate("admin", "password")
        c.mongoenginetest.add_user("username", "password")

        self.assertRaises(
            ConnectionError, connect, "testdb_uri_bad",
            host='mongodb://test:password@localhost')

        # Whilst database names can be specified in the URI, they are ignored
        # in mongoengine since the DB/connection split
        connect(host='mongodb://username:password@localhost/mongoenginetest')
        register_db('testdb_uri')

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.connection.Connection))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'testdb_uri')

    def test_register_connection(self):
        """Ensure that connections with different aliases may be registered.
        """
        register_connection('testdb')
        register_db('mongoenginetest2', 'testdb', 'testdb')

        self.assertRaises(ConnectionError, get_connection)
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.connection.Connection))

        db = get_db('testdb')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest2')


if __name__ == '__main__':
    unittest.main()
