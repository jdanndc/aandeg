import sys, getopt
from aandeg.util import load_config, file_to_json_data
from aandeg.handlers import PostgresHandler
from aandeg.read_json import read_store_class_data_json


def lambda_handler(event, context):
    dbinfo = context.get('dbinfo')
    if not dbinfo:
        raise Exception("no dbinfo")
    with PostgresHandler(dbinfo.get("db_name"), dbinfo.get("db_user"), dbinfo.get("db_pass"), dbinfo.get("db_host"),
                                                          dbinfo.get("db_port")) as pgm:
        read_store_class_data_json(event.get("payload"), pgm, is_filename=False)


if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:", ["ifile="])
    except getopt.GetoptError:
        sys.exit(2)
    filename = None
    for o, a in opts:
        if o == '-i':
            filename = a

    data = file_to_json_data(filename)
    event = {'payload': data}
    config = load_config()
    lambda_handler(event, config)
