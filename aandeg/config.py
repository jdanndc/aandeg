import json

DEFAULT_CONFIG_FILE_NAME = '.aandeg.json'

# get args from aandeg.config object
# in future, modify this to get args from AWS lambda_handler context
def args_from_context(context):
    if isinstance(context, config):
        return context.get_args()
    else:
        ret = [None, None, None, None, None]

class config():
    def __init__(self, filename=DEFAULT_CONFIG_FILE_NAME):
        self.config_data = None
        with open(filename) as json_file:
            self.config_data = json.load(json_file)

    def get_args(self):
        ret = [None, None, None, None, None]
        if (self.config_data is not None):
            dbinfo = self.config_data.get('dbinfo')
            if dbinfo is not None:
                ret = []
                for k in ['db_name', 'db_user', 'db_pass', 'db_host', 'db_port']:
                    ret.append(dbinfo.get(k))
        return ret

