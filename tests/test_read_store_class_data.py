from aandeg.handlers import CollectHandler, PostgresHandler
from aandeg.read_json import read_store_data_json, read_store_class_data_json
from aandeg.aandeg_util import make_timestamp

store_classes_test_json_str = """
{
  "manifest": "initial load",
  "store-classes": [
    {
      "type": "store-class",
      "sc_id": "store-default"
    }
  ]
}
"""


def test_read_store_class_str():
    ch = CollectHandler()
    read_store_class_data_json(store_classes_test_json_str, ch, is_filename=False)
    assert(len(ch.store_class_collect_list) == 1)
    assert(ch.store_class_collect_list[0].get('type') == 'store-class')
    assert(ch.store_class_collect_list[0].get('sc_id') == 'store-default')

def test_read_store_class_file():
    # just make sure we can read the file without errors
    filename = "../data/store_class.json"
    ch = CollectHandler()
    read_store_class_data_json(filename, ch, is_filename=True)

def test_read_store_class_db_handler():
    temp = '_' + make_timestamp()
    with PostgresHandler("aandeg", "jdann", "", "localhost", 5432, table_suffix=temp) as pgm:
        read_store_class_data_json(store_classes_test_json_str, pgm, is_filename=False)
        cursor = pgm.connection.cursor()
        cursor.execute("""SELECT * FROM {}""".format(pgm.store_class_table_name))
        assert(len(cursor.fetchall()) == 1)

