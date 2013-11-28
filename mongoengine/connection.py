import pymongo
from pymongo import Connection, ReplicaSetConnection, uri_parser


__all__ = ['ConnectionError', 'connect', 'register_connection',
           'DEFAULT_CONNECTION_NAME']


DEFAULT_CONNECTION_NAME = 'default'
DEFAULT_DB_ALIAS = 'default'


class ConnectionError(Exception):
    pass


_connection_settings = {}
_connections = {}
_dbs = {}
# Map of DB aliases to settings for the DB, including connection alias
_db_settings = {}


def register_connection(alias, host='localhost', port=27017,
                        is_slave=False, read_preference=False, slaves=None,
                        username=None, password=None, **kwargs):
    """Add a connection.

    :param alias: the name that will be used to refer to this connection
        throughout MongoEngine
    :param name: the name of the specific database to use
    :param host: the host name of the :program:`mongod` instance to connect to
    :param port: the port that the :program:`mongod` instance is running on
    :param is_slave: whether the connection can act as a slave ** Depreciated pymongo 2.0.1+
    :param read_preference: The read preference for the collection ** Added pymongo 2.1
    :param slaves: a list of aliases of slave connections; each of these must
        be a registered connection that has :attr:`is_slave` set to ``True``
    :param username: username to authenticate with
    :param password: password to authenticate with
    :param kwargs: allow ad-hoc parameters to be passed into the pymongo driver

    """
    global _connection_settings

    # Handle uri style connections
    if "://" in host:
        uri_dict = uri_parser.parse_uri(host)
        _connection_settings[alias] = {
            'host': host,
            'username': uri_dict.get('username'),
            'password': uri_dict.get('password')
        }
        _connection_settings[alias].update(kwargs)
        return

    _connection_settings[alias] = {
        'host': host,
        'port': port,
        'is_slave': is_slave,
        'slaves': slaves or [],
        'username': username,
        'password': password,
        'read_preference': read_preference
    }
    _connection_settings[alias].update(kwargs)


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    global _connections
    global _dbs

    if alias in _connections:
        conn = get_connection(alias=alias)
        conn.disconnect()
        if hasattr(conn, 'close'):
            conn.close()
        del _connections[alias]
    if alias in _dbs:
        del _dbs[alias]


def get_connection(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    global _connections
    # Connect to the database if not already connected
    if reconnect:
        disconnect(alias)

    if alias not in _connections:
        if alias not in _connection_settings:
            msg = 'Connection with alias "%s" has not been defined'
            if alias == DEFAULT_CONNECTION_NAME:
                msg = 'You have not defined a default connection'
            raise ConnectionError(msg)
        conn_settings = _connection_settings[alias].copy()

        if hasattr(pymongo, 'version_tuple'):  # Support for 2.1+
            conn_settings.pop('slaves', None)
            conn_settings.pop('is_slave', None)
            conn_settings.pop('username', None)
            conn_settings.pop('password', None)
        else:
            # Get all the slave connections
            if 'slaves' in conn_settings:
                slaves = []
                for slave_alias in conn_settings['slaves']:
                    slaves.append(get_connection(slave_alias))
                conn_settings['slaves'] = slaves
                conn_settings.pop('read_preference')

        connection_class = Connection
        if 'replicaSet' in conn_settings:
            conn_settings['hosts_or_uri'] = conn_settings.pop('host', None)
            connection_class = ReplicaSetConnection
        try:
            _connections[alias] = connection_class(**conn_settings)
        except Exception, e:
            raise ConnectionError("Cannot connect to database %s :\n%s" % (alias, e))
    return _connections[alias]


def register_db(
        db_name, db_alias=DEFAULT_DB_ALIAS,
        connection_alias=DEFAULT_CONNECTION_NAME):
    assert isinstance(db_name, basestring)
    assert isinstance(db_alias, basestring)
    assert isinstance(connection_alias, basestring)

    global _db_settings
    _db_settings[db_alias] = {
        'connection_alias': connection_alias,
        'db_name': db_name,
    }

def get_db(alias=DEFAULT_DB_ALIAS, reconnect=False, refresh=False):
    global _dbs
    global _db_settings
    db_settings = _db_settings[alias]
    if reconnect:
        disconnect(db_settings['connection_alias'])

    if alias not in _dbs or refresh:
        conn = get_connection(db_settings['connection_alias'])
        _dbs[alias] = conn[db_settings['db_name']]
        if db_settings.get('username') and db_settings.get('password'):
            _dbs[alias].authenticate(db_settings['username'],
                                     db_settings['password'])
    return _dbs[alias]


def connect(alias=DEFAULT_CONNECTION_NAME, **kwargs):
    """
    Connect to a server.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple connections are supported by using aliases.  Provide a separate
    `alias` to connect to a different instance of :program:`mongod`.

    .. versionchanged:: 0.6 - added multiple database support.
    """
    global _connections
    if alias not in _connections:
        register_connection(alias, **kwargs)

    return get_connection(alias)

# Support old naming convention
_get_connection = get_connection
_get_db = get_db
