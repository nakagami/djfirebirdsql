from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.utils import InterfaceError


class DatabaseFeatures(BaseDatabaseFeatures):
    supports_forward_references = False
    supports_tablespaces = False
    has_zoneinfo_database = False
    uses_savepoints = True
    supports_paramstyle_pyformat = True
    has_native_uuid_field = False
    has_native_duration_field = False
    supports_column_check_constraints = False
    supports_regex_backreferencing = False
    has_bulk_insert = False
    can_return_id_from_insert = True
    has_select_for_update = False
    for_update_after_from = False
    supports_transactions = True
    can_introspect_small_integer_field = True
    supports_timezones = False
    closed_cursor_error_class = InterfaceError
    has_case_insensitive_like = False
    implied_column_null = True
    uppercases_column_names = True
    ignores_table_name_case = True
    truncates_names = True
    atomic_transactions = True
    supports_select_intersection = False
    supports_select_difference = False
    supports_index_column_ordering = False
    bare_select_suffix = " FROM RDB$DATABASE"
    requires_literal_defaults = True
    supports_cast_with_precision = False
    supports_sequence_reset = False
    supports_subqueries_in_group_by = False
    supports_partially_nullable_unique_constraints = False
    supports_mixed_date_datetime_comparisons = False
    can_introspect_autofield = True
    can_introspect_duration_field = False
    supports_partial_indexes = False
    supports_over_clause = True
