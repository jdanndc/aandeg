from aandeg.config import config, args_from_context
from aandeg.handlers import PostgresHandler


def lambda_handler(event, context):
    with PostgresHandler(*args_from_context(context)) as pgm:
        pgm.drop_tables()


if __name__ == "__main__":
    lambda_handler(None, config())

