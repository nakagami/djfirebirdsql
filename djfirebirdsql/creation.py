import sys
import firebirdsql as Database
from django.db.backends.base.creation import BaseDatabaseCreation

class DatabaseCreation(BaseDatabaseCreation):
    def _get_test_db_name(self):
        return self.connection.settings_dict['NAME']

    def _check_active_connection(self, verbosity):
        if self.connection:
            if verbosity >= 1:
                print("Closing active connection")
            self.connection.close()

    def _get_connection_params(self, **overrides):
        settings_dict = self.connection.settings_dict
        conn_params = {'charset': 'UTF8'}
        conn_params['database'] = settings_dict['NAME']
        if settings_dict['HOST']:
            conn_params['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            conn_params['port'] = settings_dict['PORT']
        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        if 'ROLE' in settings_dict:
            conn_params['role'] = settings_dict['ROLE']
        conn_params.update(settings_dict['OPTIONS'])
        conn_params.update(overrides)
        return conn_params

    def _get_creation_params(self, **overrides):
        settings_dict = self.connection.settings_dict
        params = {'charset': 'UTF8'}
        if settings_dict['USER']:
            params['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            params['password'] = settings_dict['PASSWORD']

        test_settings = settings_dict.get('TEST')
        if test_settings:
            if test_settings['NAME']:
                params['database'] = settings_dict['NAME']
            if test_settings['CHARSET']:
                params['charset'] = test_settings['CHARSET']
            if test_settings['PAGE_SIZE']:
                params['page_size'] = test_settings['PAGE_SIZE']
        params.update(overrides)
        return params

    def _create_database(self, test_database_name, verbosity):
        conn = Database.create_database(
                host=self.connection.settings_dict['HOST'],
                database=self.connection.settings_dict['NAME'],
                user=self.connection.settings_dict['USER'],
                password=self.connection.settings_dict['PASSWORD'],
                page_size=self.connection.settings_dict.get('PAGE_SIZE', 32768),
        )
        conn.close()

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """"
        Internal implementation - creates the test db tables.
        """
        test_database_name = self._get_test_db_name()
        self._create_database(test_database_name, verbosity)
        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        """
        Internal implementation - remove the test db tables.
        """
        self._check_active_connection(verbosity)
        connection = Database.connect(**self._get_connection_params(database=test_database_name))
        connection.drop_database()
        connection.close()
