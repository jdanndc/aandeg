from aandeg.util.config import Config
from aandeg.administer import Administer
from aandeg.util.read_json import read_equip_class_data_json, read_prod_class_data_json, file_to_json_data
from aandeg.util.read_json import read_store_data_json, read_store_class_data_json
from aandeg.data_handler.postgres import PostgresHandler
from aandeg.util.dbutil import create_tables, drop_tables


def lambda_handler(event, context):
    conn = context.get("conn")
    drop_tables(conn)
    create_tables(conn)
    with PostgresHandler(conn) as pgh:
        read_equip_class_data_json(file_to_json_data('./data/equip_class.json'), pgh, is_filename=False)
        read_equip_class_data_json(file_to_json_data('./data/equip_class_add_one.json'), pgh, is_filename=False)
        read_prod_class_data_json(file_to_json_data('./data/product_class.json'), pgh, is_filename=False)
        read_store_class_data_json(file_to_json_data('./data/store_class.json'), pgh, is_filename=False)
        read_store_data_json(file_to_json_data('./data/store.json'), pgh, is_filename=False)
        admin = Administer(conn)
        admin.update_imputed_depends()
        admin.update_store_classes_with_all_products()


if __name__ == "__main__":
    lambda_handler(None, { "conn" : Config().connection()})

