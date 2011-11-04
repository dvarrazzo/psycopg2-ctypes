import datetime
import decimal
import math
import sys

from psycopg2ct._impl import libpq
from psycopg2ct._impl import util
from psycopg2ct._impl.exceptions import ProgrammingError
from psycopg2ct._config import PG_VERSION
from psycopg2ct.tz import LOCAL as TZ_LOCAL


adapters = {}


class _BaseAdapter(object):
    def __init__(self, wrapped_object):
        self._wrapped = wrapped_object
        self._conn = None

    def __str__(self):
        return str(self.getquoted())

    @property
    def adapted(self):
        return self._wrapped

    def _ensure_bytes(self, s):
        if self._conn:
            return self._conn.ensure_bytes(s)
        else:
            return util.ensure_bytes(s)

class ISQLQuote(_BaseAdapter):
    def getquoted(self):
        pass


class AsIs(_BaseAdapter):
    def prepare(self, connection):
        self._conn = connection

    def getquoted(self):
        return self._ensure_bytes(str(self._wrapped))


class Binary(_BaseAdapter):
    def prepare(self, connection):
        self._conn = connection

    def __conform__(self, proto):
        return self

    def getquoted(self):
        if self._wrapped is None:
            return b'NULL'

        to_length = libpq.c_uint()

        if self._conn and PG_VERSION >= 0x080104:
            data_pointer = libpq.PQescapeByteaConn(
                self._conn._pgconn, self._wrapped, len(self._wrapped),
                libpq.pointer(to_length))
        else:
            data_pointer = libpq.PQescapeBytea(
                self._wrapped, self._wrapped, libpq.pointer(to_length))

        data = data_pointer[:to_length.value - 1]
        libpq.PQfreemem(data_pointer)

        if self._conn and self._conn._equote:
            return b"E'" + data + b"'::bytea"
        else:
            return b"'" + data + b"'::bytea"


class Boolean(_BaseAdapter):
    def getquoted(self):
        return b'true' if self._wrapped else b'false'


class DateTime(_BaseAdapter):
    def getquoted(self):
        obj = self._wrapped
        if isinstance(obj, datetime.timedelta):
            # TODO: microseconds
            rv = "'%d days %d.0 seconds'::interval" % (
                int(obj.days), int(obj.seconds))
        else:
            iso = obj.isoformat()
            if isinstance(obj, datetime.datetime):
                format = 'timestamp'
                if getattr(obj, 'tzinfo', None):
                    format = 'timestamptz'
            elif isinstance(obj, datetime.time):
                format = 'time'
            else:
                format = 'date'
            rv = "'%s'::%s" % (str(iso), format)

        return self._ensure_bytes(rv)


def Date(year, month, day):
    date = datetime.date(year, month, day)
    return DateTime(date)


def DateFromTicks(ticks):
    date = datetime.datetime.fromtimestamp(ticks).date()
    return DateTime(date)


class Decimal(_BaseAdapter):
    def getquoted(self):
        if self._wrapped.is_finite():
            value = str(self._wrapped)

            # Prepend a space in front of negative numbers
            if value.startswith('-'):
                value = ' ' + value

            return self._ensure_bytes(value)

        else:
            return b"'NaN'::numeric"


class Float(ISQLQuote):
    def getquoted(self):
        n = float(self._wrapped)
        if math.isnan(n):
            return b"'NaN'::float"

        elif math.isinf(n):
            if n > 0:
                return b"'Infinity'::float"
            else:
                return b"'-Infinity'::float"

        else:
            value = repr(self._wrapped)

            # Prepend a space in front of negative numbers
            if value.startswith('-'):
                value = ' ' + value

            return self._ensure_bytes(value)


class Int(_BaseAdapter):
    def getquoted(self):
        value = str(self._wrapped)

        # Prepend a space in front of negative numbers
        if value.startswith('-'):
            value = ' ' + value

        return self._ensure_bytes(value)


class List(_BaseAdapter):

    def prepare(self, connection):
        self._conn = connection

    def getquoted(self):
        length = len(self._wrapped)
        if length == 0:
            return b"'{}'"

        quoted = [None] * length
        for i in xrange(length):
            obj = self._wrapped[i]
            quoted[i] = _getquoted(obj, self._conn)
        return b"ARRAY[%s]" % b", ".join(quoted)


class Long(_BaseAdapter):
    def getquoted(self):
        value = str(self._wrapped)

        # Prepend a space in front of negative numbers
        if value.startswith('-'):
            value = ' ' + value

        return self._ensure_bytes(value)


def Time(hour, minutes, seconds, tzinfo=None):
    time = datetime.time(hour, minutes, seconds, tzinfo=tzinfo)
    return DateTime(time)


def TimeFromTicks(ticks):
    time = datetime.datetime.fromtimestamp(ticks).time()
    return DateTime(time)


def Timestamp(year, month, day, hour, minutes, seconds, tzinfo=None):
    dt = datetime.datetime(
        year, month, day, hour, minutes, seconds, tzinfo=tzinfo)
    return DateTime(dt)


def TimestampFromTicks(ticks):
    dt = datetime.datetime.fromtimestamp(ticks, TZ_LOCAL)
    return DateTime(dt)


class QuotedString(_BaseAdapter):
    def __init__(self, obj):
        super(QuotedString, self).__init__(obj)

    def prepare(self, conn):
        self._conn = conn

    def getquoted(self):
        string = self._ensure_bytes(self._wrapped)
        length = len(string)

        to = libpq.create_string_buffer(b'\0', (length * 2) + 1)

        if self._conn and PG_VERSION >= 0x080104:
            err = libpq.c_int()
            libpq.PQescapeStringConn(
                self._conn._pgconn, to, string, length, err)

        else:
            libpq.PQescapeString(to, string, length)

        if self._conn and self._conn._equote:
            return b"E'" + to.value + b"'"
        else:
            return b"'" + to.value + b"'"


def adapt(value, proto=ISQLQuote, alt=None):
    """Return the adapter for the given value"""
    conform = getattr(value, '__conform__', None)
    if conform is not None:
        return conform(proto)

    obj_type = type(value)
    try:
        return adapters[(obj_type, proto)](value)
    except KeyError:
        for subtype in obj_type.mro()[1:]:
            try:
                return adapters[(subtype, proto)](value)
            except KeyError:
                pass

    raise ProgrammingError("can't adapt type '%s'" % obj_type.__name__)


def _getquoted(param, conn):
    """Helper method"""
    if param is None:
        return b'NULL'

    adapter = adapt(param)
    try:
        adapter.prepare(conn)
    except AttributeError:
        pass

    return adapter.getquoted()


built_in_adapters = {
    bool: Boolean,
    list: List,
    bytearray: Binary,
    float: Float,
    datetime.date: DateTime, # DateFromPY
    datetime.datetime: DateTime, # TimestampFromPy
    datetime.time: DateTime, # TimeFromPy
    datetime.timedelta: DateTime, # IntervalFromPy
    decimal.Decimal: Decimal,
}

if sys.version_info[:2] < (3, 0):
    built_in_adapters[buffer] = Binary
    built_in_adapters[int] = Int
    built_in_adapters[long] = Long
    built_in_adapters[str] = QuotedString
    built_in_adapters[unicode] = QuotedString
else:
    built_in_adapters[int] = Long
    built_in_adapters[str] = QuotedString

if sys.version_info[:2] > (2, 6):
    built_in_adapters[memoryview] = Binary

for k, v in built_in_adapters.iteritems():
    adapters[(k, ISQLQuote)] = v
