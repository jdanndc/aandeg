import sys, getopt
from aandeg.util.config import Config
from aandeg.util.read_json import read_store_data_json, file_to_json_data
from aandeg.data_handler.postgres import PostgresHandler


def lambda_handler(event, context):
    with PostgresHandler(context.get("conn")) as pgh:
        read_store_data_json(event.get("payload"), pgh, is_filename=False)


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
    lambda_handler(event, { "conn" : Config().connection()})
