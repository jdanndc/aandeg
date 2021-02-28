from aandeg.administer import Administer
from aandeg.util.config import Config


def lambda_handler(event, context):
    Administer(context.get("conn")).update_imputed_depends()


if __name__ == "__main__":
    lambda_handler(None, { "conn" : Config().connection()})

