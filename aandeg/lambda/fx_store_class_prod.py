from handler.postgres import PostgresHandler
from aandeg.config import Config, args_from_context


def lambda_handler(event, context):
    with PostgresHandler(*args_from_context(context)) as pgm:
        pgm.update_store_classes_with_all_products()


if __name__ == "__main__":
    lambda_handler(None, Config())

