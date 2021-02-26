from aandeg.handlers import PostgresHandler
from aandeg.util import load_config


def lambda_handler(event, context):
    dbinfo = context.get('dbinfo')
    if not dbinfo:
        raise Exception("no dbinfo")
    with PostgresHandler(dbinfo.get("db_name"), dbinfo.get("db_user"), dbinfo.get("db_pass"), dbinfo.get("db_host"),
                         dbinfo.get("db_port")) as pgm:
        pgm.update_store_classes_with_all_products()


if __name__ == "__main__":
    config = load_config()
    lambda_handler(None, config)

