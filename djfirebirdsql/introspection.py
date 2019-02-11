from collections import namedtuple

from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo, TableInfo,
)

FieldInfo = namedtuple('FieldInfo', FieldInfo._fields + ('identity_type',))
InfoLine = namedtuple('InfoLine', 'col_name data_type max_len num_prec num_scale extra column_default identity_type')


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        23: 'BooleanField',
        7: 'SmallIntegerField',
        8: 'IntegerField',
        10: 'FloatField',
        12: 'DateField',
        13: 'TimeField',
        14: 'CharField',
        16: 'BigIntegerField',
        27: 'FloatField',
        35: 'DateTimeField',
        37: 'CharField',
        40: 'TextField',
        260: 'BinaryField',
        261: 'TextField',
        # A NUMERIC/DECIMAL data type is stored as a SMALLINT, INTEGER or BIGINT
        # in Firebird, thus the value of RDB$FIELD_TYPE is reported. So we need
        # two additional helper data types for that to distinguish between real
        # Integer data types and NUMERIC/DECIMAL
        161: 'DecimalField',  # NUMERIC => RDB$FIELD_SUB_TYPE = 1
        162: 'DecimalField',  # DECIMAL => RDB$FIELD_SUB_TYPE = 2
        # Also, the scale value of a NUMERIC/DECIMAL fields is stored as negative
        # number in the Firebird system tables, thus we have to multiply with -1.
        # The SELECT statement in the function get_table_description takes care
        # of all of that.
    }

    def identifier_converter(self, name):
        if name:
            name = name.strip().lower()
        return name

    def sequence_list(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """select trim(rdb$relation_name),
                          trim(rdb$field_name)
                    from rdb$relation_fields
                    where rdb$identity_type is not null
                    """)
            return [{
                'table': self.identifier_converter(r[0]),
                'column': self.identifier_converter(r[1])
            } for r in cursor.fetchall()]

    def get_field_type(self, data_type, description):
        field_type = super().get_field_type(data_type, description)
        if description.identity_type:
            if field_type == 'IntegerField':
                return 'AutoField'
            elif field_type == 'BigIntegerField':
                return 'BigAutoField'
        return field_type

    def get_table_list(self, cursor):
        "Returns a list of table names in the current database."
        cursor.execute("""
            select
                trim(rdb$relation_name),
                case when RDB$VIEW_BLR IS NULL then 't' else 'v' end as rel_type
            from rdb$relations
            where rdb$system_flag=0
            order by 1 """)
        return [TableInfo(self.identifier_converter(row[0]), row[1]) for row in cursor.fetchall()]

    def get_table_description(self, cursor, table_name):
        """
        Returns a description of the table, with the DB-API cursor.description interface.
        Must return a 'FieldInfo' struct 'name type_code display_size internal_size precision scale null_ok'
        """
        cursor.execute("""
            select
              trim(rf.rdb$field_name)
              , case
                  when (f.rdb$field_type in (7,8,16)) and (f.rdb$field_sub_type > 0) then
                    160 + f.rdb$field_sub_type
                  when (f.rdb$field_type = 261) then
                    260 + f.rdb$field_sub_type
                  else
                    f.rdb$field_type
                end as type_code

              , case
                  when (f.rdb$field_type in (14,37)) then
                    f.rdb$character_length
                  else
                    f.rdb$field_length
                end as field_length
              , f.rdb$field_precision
              , f.rdb$field_scale * -1
              , rf.rdb$null_flag
              , rf.rdb$default_source
              , rf.rdb$identity_type
            from
              rdb$relation_fields rf join rdb$fields f on (rf.rdb$field_source = f.rdb$field_name)
            where
              rf.rdb$relation_name = '%s'
            order by
              rf.rdb$field_position
            """ % (table_name.strip().upper(),))
        items = []
        for r in cursor.fetchall():
            # name type_code display_size internal_size precision scale null_ok, default, identity_type
            items.append(FieldInfo(self.identifier_converter(r[0]), r[1], r[2], r[2] or 0, r[3], r[4], not (r[5] == 1), r[6], r[7]))
        return items

    def _name_to_index(self, cursor, table_name):
        """Return a dictionary of {field_name: field_index} for the given table.
           Indexes are 0-based.
        """
        return dict([(self.identifier_converter(d[0]), i) for i, d in enumerate(self.get_table_description(cursor, table_name))])

    def _get_references(self, table_name):
        """
        Foreign Key constraint reference.
        constraint name, table, column, referenecd table, referenced column
        table_name: refernced tbale name
        """
        with self.connection.cursor() as cursor:
            references = []
            cursor.execute("""
                select
                    refc.rdb$constraint_name,
                    i.rdb$relation_name
                from rdb$index_segments s
                left join rdb$indices i on i.rdb$index_name = s.rdb$index_name
                left join rdb$relation_constraints rc on rc.rdb$index_name = s.rdb$index_name
                left join rdb$ref_constraints refc on rc.rdb$constraint_name = refc.rdb$constraint_name
                left join rdb$relation_constraints rc2 on rc2.rdb$constraint_name = refc.rdb$const_name_uq
                left join rdb$indices i2 on i2.rdb$index_name = rc2.rdb$index_name
                left join rdb$index_segments s2 on i2.rdb$index_name = s2.rdb$index_name
                WHERE RC.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
                and i2.rdb$relation_name = '%s' """ % (table_name.strip().upper(),))
            return [(self.identifier_converter(r[0]), self.identifier_converter(r[1].strip())) for r in cursor.fetchall()]

    def get_key_columns(self, cursor, table_name):
        """
        Backends can override this to return a list of (column_name, referenced_table_name,
        referenced_column_name) for all key columns in given table.
        """
        key_columns = []
        cursor.execute("""
            select
                s.rdb$field_name as column_name,
                i2.rdb$relation_name as referenced_table_name,
                s2.rdb$field_name as referenced_column_name
            from rdb$index_segments s
            left join rdb$indices i on i.rdb$index_name = s.rdb$index_name
            left join rdb$relation_constraints rc on rc.rdb$index_name = s.rdb$index_name
            left join rdb$ref_constraints refc on rc.rdb$constraint_name = refc.rdb$constraint_name
            left join rdb$relation_constraints rc2 on rc2.rdb$constraint_name = refc.rdb$const_name_uq
            left join rdb$indices i2 on i2.rdb$index_name = rc2.rdb$index_name
            left join rdb$index_segments s2 on i2.rdb$index_name = s2.rdb$index_name
            WHERE RC.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
            and i.rdb$relation_name = '%s' """ % (table_name.strip().upper(),))

        for r in cursor.fetchall():
            key_columns.append((self.identifier_converter(r[0]), self.identifier_converter(r[1]), self.identifier_converter(r[2])))
        return key_columns

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all relationships to the given table.
        """
        constraints = self.get_key_columns(cursor, table_name)
        relations = {}
        for my_fieldname, other_table, other_field in constraints:
            relations[my_fieldname] = (other_field, other_table)
        return relations

    def get_constraints(self, cursor, table_name):
        """
        Retrieves any constraints or keys (unique, pk, fk, check, index)
        across one or more columns.

        Returns a dict mapping constraint names to their attributes,
        where attributes is a dict with keys:
         * columns: List of columns this covers
         * primary_key: True if primary key, False otherwise
         * unique: True if this is a unique constraint, False otherwise
         * foreign_key: (table, column) of target, or None
         * check: True if check constraint, False otherwise
         * index: True if index, False otherwise.

        Some backends may return special constraint names that don't exist
        if they don't name constraints of a certain type (e.g. SQLite)
        """
        # TODO: FIX check constraint
        constraints = {}

        cursor.execute("""
        SELECT
          case
            when rc.RDB$CONSTRAINT_NAME is not null then rc.RDB$CONSTRAINT_NAME
            else i.RDB$INDEX_NAME
          end as constraint_name,

          case
            when rc.RDB$CONSTRAINT_TYPE is not null then rc.RDB$CONSTRAINT_TYPE
            else 'INDEX'
          end AS constraint_type,

          s.RDB$FIELD_NAME AS field_name,
          i2.RDB$RELATION_NAME AS references_table,
          s2.RDB$FIELD_NAME AS references_field,
          i.RDB$UNIQUE_FLAG
        FROM RDB$INDEX_SEGMENTS s
        LEFT JOIN RDB$INDICES i ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
        LEFT JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS rc2 ON rc2.RDB$CONSTRAINT_NAME = refc.RDB$CONST_NAME_UQ
        LEFT JOIN RDB$INDICES i2 ON i2.RDB$INDEX_NAME = rc2.RDB$INDEX_NAME
        LEFT JOIN RDB$INDEX_SEGMENTS s2 ON i2.RDB$INDEX_NAME = s2.RDB$INDEX_NAME
        WHERE i.RDB$RELATION_NAME = '%s'
        ORDER BY s.RDB$FIELD_POSITION
        """ % (table_name.strip().upper(),))
        for constraint_name, constraint_type, column, other_table, other_column, unique in cursor.fetchall():
            primary_key = False
            foreign_key = None
            check = False
            index = False
            constraint = self.identifier_converter(constraint_name)
            constraint_type = constraint_type.strip()
            column = self.identifier_converter(column)
            if other_table:
                other_table = self.identifier_converter(other_table)
            if other_column:
                other_column = self.identifier_converter(other_column)

            if constraint_type == 'PRIMARY KEY':
                primary_key = True
            elif constraint_type == 'UNIQUE':
                unique = True
            elif constraint_type == 'FOREIGN KEY':
                foreign_key = (other_table, other_column,)
            elif constraint_type == 'INDEX':
                index = True

            if constraint not in constraints:
                constraints[constraint] = {
                    "columns": [],
                    "primary_key": primary_key,
                    "unique": unique,
                    "foreign_key": foreign_key,
                    "check": check,
                    "index": index,
                    "type": 'idx' if index else '',
                }
            # Record the details
            constraints[constraint]['columns'].append(column)

        if not constraints:
            raise ValueError("'%s' is not found." % table_name)
        return constraints

    def _get_field_indexes(self, table_name, field_name):
        """
          Return a list of index (index_name, type, constraint_name)
        """
        with self.connection.cursor() as cursor:

            table = "'%s'" % table_name.upper()
            field = "'%s'" % field_name.upper()
            cursor.execute("""
                select
                    s.rdb$index_name,
                    rc.rdb$constraint_name,
                    rc.rdb$constraint_type
                from rdb$index_segments s
                left join rdb$indices i on i.rdb$index_name = s.rdb$index_name
                left join rdb$relation_constraints rc on rc.rdb$index_name = s.rdb$index_name
                where i.rdb$relation_name = '%s'
                and s.rdb$field_name = '%s'
                order by s.rdb$field_position """ % (table.strip().upper(), field.strip().upper())
            )
    
            return [(self.identifier_converter(i[0]), self.identifier_converter(i[1]), self.identifier_converter(i[2])) for i in cursor.fetchall()]
