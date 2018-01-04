import os
import subprocess

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):

    @classmethod
    def runshell_db(cls, conn_params):
        if os.path.exists('/usr/bin/isql-fb'):
            executable_name = 'isql-fb'
        else:
            executable_name = 'isql'

        args = [executable_name]

        args.append(params['database'])

        if params['user']:
            args += ["-u", params['user']]
        if params['password']:
            args += ["-p", params['password']]
        if 'role' in params:
            args += ["-r", params['role']]

        subprocess.check_call(args)

    def runshell(self):
        DatabaseClient.runshell_db(self.connection.get_connection_params())
