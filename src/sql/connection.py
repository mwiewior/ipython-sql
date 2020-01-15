import sqlalchemy
import os
import re
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import subprocess

class ConnectionError(Exception):
    pass


def rough_dict_get(dct, sought, default=None):
    '''
    Like dct.get(sought), but any key containing sought will do.

    If there is a `@` in sought, seek each piece separately.
    This lets `me@server` match `me:***@myserver/db`
    '''
  
    sought = sought.split('@') 
    for (key, val) in dct.items():
        if not any(s.lower() not in key.lower() for s in sought):
            return val
    return default


class Connection(object):
    current = None
    connections = {}

    def is_presto(self, url):
        backend = sqlalchemy.engine.url.make_url(url).get_backend_name()
        return backend == "presto"

    @classmethod
    def tell_format(cls):
        return """Connection info needed in SQLAlchemy format, example:
               postgresql://username:password@hostname/dbname
               or an existing connection: %s""" % str(cls.connections.keys())

    def __init__(self, connect_str=None):
        engine = None
        try:
            if self.is_presto(connect_str):
                presto_url = os.environ['PRESTO_URL']
                parsed_conn_string = connect_str.split('/')
                db = parsed_conn_string[-1]
                catalog = parsed_conn_string[-2]
                UID = str(os.getuid())
                cmd = 'klist |  grep -m 1 -Po "[_a-zA-Z0-9./-]+@[_a-zA-Z0-9.]+$"'
                ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                principal = ps.communicate()[0].decode("utf-8").strip()
                args = {'protocol': 'https', \
                    'KerberosRemoteServiceName': os.environ['KERBEROS_REMOTES_SERVICE_NAME'], \
                    'KerberosConfigPath':os.environ['KERBEROS_CONFIG_PATH'], \
                    'KerberosPrincipal': principal, \
                    'KerberosCredentialCachePath': f'/tmp/krb5cc_{UID}', \
                    'requests_kwargs': {'verify': False} \
                    }
                engine = sqlalchemy.create_engine(f"{presto_url}/{catalog}/{db}", connect_args=args)
            else:
                engine = sqlalchemy.create_engine(connect_str)
        except: # TODO: bare except; but what's an ArgumentError?
            print(self.tell_format())
            raise
        self.dialect = engine.url.get_dialect()
        self.metadata = sqlalchemy.MetaData(bind=engine)
        self.name = self.assign_name(engine)
        self.session = engine.connect()
        self.connections[repr(self.metadata.bind.url)] = self
        Connection.current = self

    @classmethod
    def set(cls, descriptor):
        "Sets the current database connection"

        if descriptor:
            if isinstance(descriptor, Connection):
                cls.current = descriptor
            else:
                existing = rough_dict_get(cls.connections, descriptor)
            cls.current = existing or Connection(descriptor)
        else:
            if cls.connections:
                print(cls.connection_list())
            else:
                if os.getenv('DATABASE_URL'):
                    cls.current = Connection(os.getenv('DATABASE_URL'))
                else:
                    raise ConnectionError('Environment variable $DATABASE_URL not set, and no connect string given.')
        return cls.current

    @classmethod
    def assign_name(cls, engine):
        name = '%s@%s' % (engine.url.username or '', engine.url.database)
        return name

    @classmethod
    def connection_list(cls):
        result = []
        for key in sorted(cls.connections):
            engine_url = cls.connections[key].metadata.bind.url # type: sqlalchemy.engine.url.URL
            if cls.connections[key] == cls.current:
                template = ' * {}'
            else:
                template = '   {}'
            result.append(template.format(engine_url.__repr__()))
        return '\n'.join(result)
