from aandeg.config import Config, args_from_context
from handler.postgres import PostgresHandler


def lambda_handler(event, context):
    with PostgresHandler(*args_from_context(context)) as pgm:
        pgm.drop_tables()


if __name__ == "__main__":
    lambda_handler(None, Config())

