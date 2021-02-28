import json
from aandeg.util.exceptions import MissingDataError
from aandeg.util.dbutil import create_connection

DEFAULT_CONFIG_FILE_NAME = '.aandeg.json'
REQUIRED_KEYS = ['db_name', 'db_user', 'db_pass', 'db_host', 'db_port']


# get args from aandeg.config object
# in future, modify this to get args from AWS lambda_handler context
def args_from_context(context):
    if isinstance(context, Config):
        return context.get_args()
    else:
        ret = [None, None, None, None, None]


class Config():
    def __init__(self, filename=DEFAULT_CONFIG_FILE_NAME):
        self.config_data = None
        with open(filename) as json_file:
            self.config_data = json.load(json_file)
        dbinfo = self.config_data.get('dbinfo')
        if dbinfo is None:
            raise MissingDataError("config missing dbinfo section")
        for k in REQUIRED_KEYS:
            if k not in dbinfo:
                raise MissingDataError("config missing key {}".format(k))

    def get_args(self):
        ret = [None, None, None, None, None]
        if self.config_data is not None:
            dbinfo = self.config_data.get('dbinfo')
            if dbinfo is not None:
                ret = []
                for k in REQUIRED_KEYS:
                    ret.append(dbinfo.get(k))
        return ret

    def create_connection(self):
        conn = create_connection(*self.get_args())
        return conn

