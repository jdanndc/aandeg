from aandeg.handlers import CollectHandler, PostgresHandler
from aandeg.read_json import read_store_data_json, read_store_class_data_json
from aandeg.util import aandeg_config

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
    ch = CollectHandler()
    read_store_class_data_json("./data/store_class.json", ch, is_filename=True)

def test_read_store_class_db_handler():
    with PostgresHandler(*aandeg_config().get_args(), is_testing=True) as pgm:
        read_store_class_data_json(store_classes_test_json_str, pgm, is_filename=False)
        cursor = pgm.connection.cursor()
        cursor.execute("""SELECT * FROM store_class""")
        assert(len(cursor.fetchall()) == 1)

