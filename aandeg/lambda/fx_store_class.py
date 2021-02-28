import sys, getopt
from aandeg.util import file_to_json_data
from aandeg.config import Config, args_from_context
from handler.postgres import PostgresHandler
from aandeg.read_json import read_store_class_data_json


def lambda_handler(event, context):
    with PostgresHandler(*args_from_context(context)) as pgm:
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
    lambda_handler(event, Config())
