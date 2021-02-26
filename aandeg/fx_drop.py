from aandeg.util import load_config
from aandeg.handlers import PostgresHandler


def lambda_handler(event, context):
    dbinfo = context.get('dbinfo')
    if not dbinfo:
        raise Exception("no dbinfo")
    with PostgresHandler(dbinfo.get("db_name"), dbinfo.get("db_user"), dbinfo.get("db_pass"), dbinfo.get("db_host"),
                         dbinfo.get("db_port")) as pgm:
        pgm.drop_tables()


if __name__ == "__main__":
    config = load_config()
    lambda_handler(None, config)

