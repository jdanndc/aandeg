from aandeg.data_handler.postgres import PostgresHandler
from aandeg.data_handler.collect import CollectHandler
from aandeg.util.read_json import read_store_class_data_json
from aandeg.util.config import Config

store_classes_test_json_str = """
{
  "manifest": "initial load",
  "store_classes": [
    {
      "type": "store_class",
      "sc_id": "store_default"
    }
  ]
}
"""


def test_read_store_class_str():
    ch = CollectHandler()
    read_store_class_data_json(store_classes_test_json_str, ch, is_filename=False)
    assert(len(ch.store_class_collect_list) == 1)
    assert(ch.store_class_collect_list[0].get('type') == 'store_class')
    assert(ch.store_class_collect_list[0].get('sc_id') == 'store_default')


def test_read_store_class_file():
    # just make sure we can read the file without errors
    ch = CollectHandler()
    read_store_class_data_json("./data/store_class.json", ch, is_filename=True)


def test_read_store_class_db_handler():
    with PostgresHandler(Config().create_connection(), is_testing=True) as pgm:
        read_store_class_data_json(store_classes_test_json_str, pgm, is_filename=False)
        cursor = pgm.connection.cursor()
        cursor.execute("""SELECT * FROM store_class""")
        assert(len(cursor.fetchall()) == 1)

