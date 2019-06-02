import datetime
import uuid
import collections
from django.utils import timezone
from django.db.utils import InterfaceError

try:
    import firebirdsql as Database
except ImportError as e:
    raise ImproperlyConfigured("Error loading firebirdsql module: %s" % e)

def _quote_value(value):
    import binascii
    if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
        return "'%s'" % value
    elif isinstance(value, uuid.UUID):
        return "'%s'" % value.hex
    elif isinstance(value, str):
        return "'%s'" % value.replace("\'", "\'\'")
    elif isinstance(value, (bytes, bytearray, memoryview)):
        return "x'%s'" % binascii.hexlify(value).decode('ascii')
    elif value is None:
        return "NULL"
    else:
        return str(value)

def convert_sql(query, params):
    if params is None:
        pass
    elif isinstance(params, dict):
        converted_params = {}
        for k, v in params.items():
            if isinstance(v, datetime.datetime) and timezone.is_aware(v):
                v = v.astimezone(timezone.utc).replace(tzinfo=None)
            converted_params[k] = _quote_value(v)
        query = query % converted_params
    elif isinstance(params, (list, tuple)):
        converted_params = []
        for p in params:
            v = p
            if isinstance(v, datetime.datetime) and timezone.is_aware(v):
                v = v.astimezone(timezone.utc).replace(tzinfo=None)
            converted_params.append(_quote_value(v))
        if len(converted_params) == 1:
            query = query % converted_params[0]
        else:
            query = query % tuple(converted_params)
    return query


class FirebirdCursorWrapper(Database.Cursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rows = collections.deque()
        self.closed = False
        self.query = ''

    def execute(self, query, params=None):
        if self.closed:
            raise InterfaceError('Cursor is closed')
        self.query = convert_sql(query, params)
        super().execute(self.query)
        self._rows = collections.deque(super().fetchall())
        if self._transaction._autocommit:
            self._transaction._connection.commit()

    def executemany(self, query, param_list):
        if self.closed:
            raise InterfaceError('Cursor is closed')
        for params in param_list:
            super().execute(convert_sql(query, params))

    @property
    def description(self):
        if not self.stmt:
            return None
        return [(
            x.aliasname.lower(), x.sqltype, x.display_length(), x.io_length(),
            x.precision(), x.sqlscale, True if x.null_ok else False
        ) for x in self.stmt.xsqlda]

    def fetchone(self):
        if len(self._rows):
            return self._rows.popleft()
        return None

    def fetchmany(self, size=1):
        rs = []
        for i in range(size):
            r = self.fetchone()
            if not r:
                break
            rs.append(r)
        return rs

    def fetchall(self):
        r = list(self._rows)
        self._rows.clear()
        return r

    def close(self):
        super().close()
        self.closed = True
