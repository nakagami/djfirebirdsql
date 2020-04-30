import os
import subprocess

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):

    @classmethod
    def settings_to_cmd_args(cls, settings_dict, parameters):
        if os.path.exists('/usr/bin/isql-fb'):
            executable_name = 'isql-fb'
        else:
            executable_name = 'isql'

        args = [executable_name]

        database = settings_dict['OPTIONS'].get('database', settings_dict['NAME'])
        user = settings_dict['OPTIONS'].get('user', settings_dict['USER'])
        password = settings_dict['OPTIONS'].get('password', settings_dict['PASSWORD'])
        role = settings_dict['OPTIONS'].get('ROLE')
        host = settings_dict['OPTIONS'].get('host', settings_dict['HOST'])
        port = settings_dict['OPTIONS'].get('port', settings_dict['PORT'])

        args.append(database)

        if user:
            args += ["-u", user]
        if password:
            args += ["-p", password]
        if role:
            args += ["-r", role]

        args.extend(parameters)

    def runshell(self, parameters):
        args = DatabaseClient.settings_to_cmd_args(self.connection.settings_dict, parameters)
        subprocess.run(args, check=True)
