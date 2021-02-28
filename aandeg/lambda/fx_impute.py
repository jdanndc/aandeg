from handler.postgres import PostgresHandler
from aandeg.config import config, args_from_context


def lambda_handler(event, context):
    with PostgresHandler(*args_from_context(context)) as pgm:
        pgm.update_imputed_depends()


if __name__ == "__main__":
    lambda_handler(None, config())

