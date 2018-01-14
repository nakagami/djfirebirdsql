import uuid
import datetime
import firebirdsql as Database

from django.conf import settings
from django.db.backends.utils import truncate_name
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from django.utils.encoding import force_text
from django.db.utils import DatabaseError

class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "djfirebirdsql.compiler"
    cast_char_field_without_max_length = 'blob subtype text'

    integer_field_ranges = {
        'SmallIntegerField': (-32768, 32767),
        'IntegerField': (-2147483648, 2147483647),
        'BigIntegerField': (-9223372036854775808, 9223372036854775807),
        'PositiveSmallIntegerField': (0, 32767),
        'PositiveIntegerField': (0, 2147483647),
    }

    def cache_key_culling_sql(self):
        return """
            SELECT cache_key
              FROM (SELECT cache_key, rank() OVER (ORDER BY cache_key) AS rank FROM %s)
             WHERE rank = %%s + 1
        """

    def unification_cast_sql(self, output_field):
        internal_type = output_field.get_internal_type()
        if internal_type in ("GenericIPAddressField", "IPAddressField", "TimeField", "UUIDField"):
            return 'CAST(%%s AS %s)' % output_field.db_type(self.connection).split('(')[0]
        return '%s'

    def date_extract_sql(self, lookup_type, field_name):
        if lookup_type == 'week_day':
            return "EXTRACT(WEEKDAY FROM %s) + 1" % field_name
        return "EXTRACT(%s FROM %s)" % (lookup_type.upper(), field_name)

    def date_interval_sql(self, timedelta):
        return timedelta

    def format_for_duration_arithmetic(self, sql):
        """Do nothing since formatting is handled in the custom function."""
        return sql

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type == 'year':
            sql = "EXTRACT(year FROM %s)||'-01-01 00:00:00'" % field_name
        elif lookup_type == 'month':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-01 00:00:00'" % (field_name, field_name)
        elif lookup_type == 'day':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-'||EXTRACT(day FROM %s)||' 00:00:00'" % (field_name, field_name, field_name)
        else:
            sql = field_name
        return "CAST(%s AS TIMESTAMP)" % sql

    def datetime_cast_date_sql(self, field_name, tzname):
        return 'CAST(%s AS DATE)' % field_name

    def datetime_cast_time_sql(self, field_name, tzname):
        return 'CAST(%s AS TIME)' % field_name

    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        if lookup_type == 'week_day':
            sql = "EXTRACT(WEEKDAY FROM %s) + 1" % field_name
        else:
            sql = "EXTRACT(%s FROM %s)" % (lookup_type.upper(), field_name)
        return sql

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        """
        Given a lookup_type of 'year', 'month', 'day', 'hour', 'minute' or
        'second', returns the SQL that truncates the given datetime field
        field_name to a datetime object with only the given specificity, and
        a tuple of parameters.
        """
        year = "EXTRACT(year FROM %s)" % field_name
        month = "EXTRACT(month FROM %s)" % field_name
        day = "EXTRACT(day FROM %s)" % field_name
        hh = "EXTRACT(hour FROM %s)" % field_name
        mm = "EXTRACT(minute FROM %s)" % field_name
        ss = "EXTRACT(second FROM %s)" % field_name
        if lookup_type == 'year':
            sql = "%s||'-01-01 00:00:00'" % year
        elif lookup_type == 'month':
            sql = "%s||'-'||%s||'-01 00:00:00'" % (year, month)
        elif lookup_type == 'day':
            sql = "%s||'-'||%s||'-'||%s||' 00:00:00'" % (year, month, day)
        elif lookup_type == 'hour':
            sql = "%s||'-'||%s||'-'||%s||' '||%s||':00:00'" % (year, month, day, hh)
        elif lookup_type == 'minute':
            sql = "%s||'-'||%s||'-'||%s||' '||%s||':'||%s||':00'" % (year, month, day, hh, mm)
        elif lookup_type == 'second':
            sql = "%s||'-'||%s||'-'||%s||' '||%s||':'||%s||':'||%s" % (year, month, day, hh, mm, ss)
        return "CAST(%s AS TIMESTAMP)" % sql

    def time_trunc_sql(self, lookup_type, field_name):
        fields = {
            'hour': '%%H:00:00',
            'minute': '%%H:%%i:00',
            'second': '%%H:%%i:%%s',
        }  # Use double percents to escape.
        if lookup_type in fields:
            format_str = fields[lookup_type]
            return "CAST(DATE_FORMAT(%s, '%s') AS TIME)" % (field_name, format_str)
        else:
            return "TIME(%s)" % (field_name)

    def no_limit_value(self):
        return None

    def quote_name(self, name):
        name = '_'.join(name.split(' '))
        if not name.startswith('"') and not name.endswith('"'):
            name = '"%s"' % truncate_name(name, self.max_name_length())
        return name.upper()

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        if tables:
            sql = ['%s %s %s;' %
                    (style.SQL_KEYWORD('DELETE'),
                     style.SQL_KEYWORD('FROM'),
                     style.SQL_TABLE(self.quote_name(table))
                     ) for table in tables]
            return sql
        else:
            return []

    def sequence_reset_by_name_sql(self, style, sequences):
        sql = []
        query = "ALTER TABLE %s ALTER COLUMN %s RESTART"
        for sequence_info in sequences:
            table_name = sequence_info['table']
            column_name = sequence_info['column']

            if not column_name:
                column_name = 'id'

            sql.append(query % (
                self.quote_name(table_name),
                self.quote_name(column_name),
            ))
        return sql

    def sequence_reset_sql(self, style, model_list):
        from django.db import models
        output = []
        query = "ALTER TABLE %s ALTER COLUMN %s RESTART"
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    output.append(query % (
                        self.quote_name(model._meta.db_table),
                        self.quote_name(f.column),
                    ))
                    # Only one AutoField is allowed per model, so don't
                    # continue to loop
                    break
            for f in model._meta.many_to_many:
                if not f.remote_field.through:
                    output.append(query % (
                        self.quote_name(f.m2m_db_table()),
                        self.quote_name('id'),
                    ))
        return output

    def max_name_length(self):
        return 63

    def pk_default_value(self):
        return "DEFAULT"

    def return_insert_id(self):
        return "RETURNING %s", ()

    def random_function_sql(self):
        """
        Returns a SQL expression that returns a random value.
        """
        return 'RAND()'

    def prep_for_iexact_query(self, x):
        return x

    def adapt_datetimefield_value(self, value):
        """
        Transform a datetime value to an object compatible with what is expected
        by the backend driver for datetime columns.
        """
        if value is None:
            return None

        # Firebird doesn't support tz-aware datetimes
        if timezone.is_aware(value):
            if settings.USE_TZ:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                raise ValueError("Firebird backend does not support timezone-aware datetimes when USE_TZ is False.")

        # Replaces 6 digits microseconds to 4 digits allowed in Firebird
        if isinstance(value, datetime.datetime):
            value = str(value)
        if isinstance(value, str):
            value = value[:24]
        return force_text(value)

    def adapt_timefield_value(self, value):
        if value is None:
            return None

        # Firebird doesn't support tz-aware times
        if timezone.is_aware(value):
            raise ValueError("Firebird backend does not support timezone-aware times.")
        # Replaces 6 digits microseconds to 4 digits allowed in Firebird
        if isinstance(value, datetime.time):
            value = str(value)
        if isinstance(value, str):
            value = value[:13]
        return force_text(value)

    def combine_expression(self, connector, sub_expressions):
        lhs, rhs = sub_expressions
        if connector == '%%':
            return 'MOD(%s)' % ','.join(sub_expressions)
        elif connector == '&':
            return 'BIN_AND(%s)' % ','.join(sub_expressions)
        elif connector == '|':
            return 'BIN_AND(-%(lhs)s-1,%(rhs)s)+%(lhs)s' % {'lhs': lhs, 'rhs': rhs}
        elif connector == '<<':
            return '(%(lhs)s * POWER(2, %(rhs)s))' % {'lhs': lhs, 'rhs': rhs}
        elif connector == '>>':
            return 'FLOOR(%(lhs)s / POWER(2, %(rhs)s))' % {'lhs': lhs, 'rhs': rhs}
        elif connector == '^':
            return 'POWER(%s)' % ','.join(sub_expressions)
        return super().combine_expression(connector, sub_expressions)


    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'DateTimeField':
            converters.append(self.convert_datetimefield_value)
        elif internal_type in ['IPAddressField', 'GenericIPAddressField']:
            converters.append(self.convert_ipfield_value)
        elif internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        return converters

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            if settings.USE_TZ:
                value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_ipfield_value(self, value, expression, connection):
        if value is not None:
            value = value.strip()
        return value

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value

    def combine_duration_expression(self, connector, sub_expressions):
        if connector not in ['+', '-']:
            raise DatabaseError('Invalid connector for timedelta: %s.' % connector)

        sql, timedelta = sub_expressions
        sign = 1 if connector == '+' else -1
        if isinstance(timedelta, str):
            return 'DATEADD(MILLISECOND, %s%s/1000000, %s)' % (connector, timedelta, sql)
        elif timedelta.days:
            unit = 'day'
            value = timedelta.days
        elif timedelta.seconds:
            unit = 'second'
            value = ((timedelta.days * 86400) + timedelta.seconds)
        elif timedelta.microseconds:
            unit = 'millisecond'
            value = timedelta.microseconds // 1000
        else:
            unit = 'millisecond'
            value = 0
        return 'DATEADD(%s %s TO %s)' % (value * sign, unit, sql)

    def year_lookup_bounds_for_datetime_field(self, value):
        first = '%s-01-01 00:00:00' % value
        second = '%s-12-31 23:59:59.9999' % value
        return [first, second]

    def year_lookup_bounds_for_date_field(self, value):
        first = '%s-01-01' % value
        second = '%s-12-31' % value
        return [first, second]

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"

    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%s CONTAINING %%s' % self.quote_name(field_name)

    def max_in_list_size(self):
        return 1500
