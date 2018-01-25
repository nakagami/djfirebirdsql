from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.utils import InterfaceError


class DatabaseFeatures(BaseDatabaseFeatures):
    supports_forward_references = False
    supports_tablespaces = False
    supports_timezones = False
    has_zoneinfo_database = False
    uses_savepoints = True
    supports_paramstyle_pyformat = True
    connection_persists_old_columns = True
    can_rollback_ddl = False
    has_native_uuid_field = False
    has_native_duration_field = False
    supports_column_check_constraints = False
    uppercases_column_names = True
    supports_regex_backreferencing = False
    has_bulk_insert = False
    can_return_id_from_insert = True
    has_native_duration_field = False
    has_select_for_update = False
    for_update_after_from = False
    can_release_savepoints = True
    supports_transactions = True
    can_introspect_small_integer_field = True
    supports_timezones = False
    closed_cursor_error_class = InterfaceError
    has_case_insensitive_like = False
    implied_column_null = True
    uppercases_column_names = True
    ignores_table_name_case = True
    truncates_names = True
    atomic_transactions = False
    supports_select_intersection = False
    supports_select_difference = False
    supports_microsecond_precision = False
    supports_index_column_ordering = False
    bare_select_suffix = " FROM RDB$DATABASE"
    requires_literal_defaults = False
