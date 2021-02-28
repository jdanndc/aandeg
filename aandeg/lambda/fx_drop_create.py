from aandeg.util.config import Config
from aandeg.util.dbutil import create_tables, drop_tables


def lambda_handler(event, context):
    conn = context.get("conn")
    drop_tables(conn)
    create_tables(conn)


if __name__ == "__main__":
    lambda_handler(None, { "conn" : Config().create_connection()})

