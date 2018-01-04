from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Create the SQL for this query. Return the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        result, params = super().as_sql(with_limits=False, with_col_aliases=with_col_aliases)
        if with_limits:
            if self.query.low_mark:
                result += ' OFFSET %d ROWS ' % self.query.low_mark
            if self.query.high_mark is not None:
                result += ' FETCH NEXT %d ROWS ONLY' % (self.query.high_mark - self.query.low_mark)
        return result, params

class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
