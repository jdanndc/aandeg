import sys, getopt
import json
from os import path
from aandeg.handlers import PostgresHandler
from aandeg.read_json import read_equip_data_json


def lambda_handler(event, context):
    dbinfo = context.get('dbinfo')
    if not dbinfo:
        raise Exception("no dbinfo")
    with PostgresHandler(dbinfo.get("db_name"), dbinfo.get("db_user"), dbinfo.get("db_pass"), dbinfo.get("db_host"),
                                                          dbinfo.get("db_port")) as pgm:
        read_equip_data_json(event.get("payload"), pgm, is_filename=False)
    #print(event)
    #print(context)


if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:", ["ifile="])
    except getopt.GetoptError:
        sys.exit(2)
    filename = None
    for o, a in opts:
        if o == '-i':
            filename = a

    if filename:
        if path.exists(filename):
            with open(filename, 'r') as file:
                data = file.read()
                event = {'payload': data}
                config = {}
                with open('.aandeg.json') as json_file:
                    config = json.load(json_file)
                lambda_handler(event, config)
        else:
            raise Exception("file not found {}".format(filename))
