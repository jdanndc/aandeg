from aandeg.util.config import Config
from aandeg.administer import Administer


def lambda_handler(event, context):
    Administer(context.get("conn")).update_store_classes_with_all_products()


if __name__ == "__main__":
    lambda_handler(None, { "conn" : Config().connection()})

