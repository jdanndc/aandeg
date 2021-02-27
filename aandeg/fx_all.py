from aandeg.config import config, args_from_context
from aandeg.handlers import PostgresHandler
from aandeg.util import file_to_json_data
from aandeg.read_json import read_equip_class_data_json, read_prod_class_data_json
from aandeg.read_json import read_store_data_json, read_store_class_data_json


def lambda_handler(event, context):
    with PostgresHandler(*args_from_context(context)) as pgm:
        read_equip_class_data_json(file_to_json_data('./data/equip_class.json'), pgm, is_filename=False)
        read_equip_class_data_json(file_to_json_data('./data/equip_class_add_one.json'), pgm, is_filename=False)
        pgm.update_imputed_depends()
        read_prod_class_data_json(file_to_json_data('./data/product_class.json'), pgm, is_filename=False)
        read_store_class_data_json(file_to_json_data('./data/store_class.json'), pgm, is_filename=False)
        read_store_data_json(file_to_json_data('./data/store.json'), pgm, is_filename=False)
        pgm.update_store_classes_with_all_products()


if __name__ == "__main__":
    lambda_handler(None, config())

