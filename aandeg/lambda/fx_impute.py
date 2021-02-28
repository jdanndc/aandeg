from aandeg.model import Model
from aandeg.util.config import Config


def lambda_handler(event, context):
    Model(context.get("conn")).update_imputed_depends()


if __name__ == "__main__":
    lambda_handler(None, { "conn" : Config().create_connection()})

