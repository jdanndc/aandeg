import traceback

from aandeg.data_handler.base import BaseHandler
from aandeg.data_handler.base import EC_DEPEND_TYPE_DEFINED
from aandeg.util.dbutil import create_tables


# TODO:
# create a class RDBHandler(BaseHandler)
# that handles all the common RDB functionality between AWS RDS and local Postgres
# and then one instance for PostgresHandler and one for AWS RDS
class PostgresHandler(BaseHandler):
    # TODO: handle the password more safely
    def __init__(self, db_conn, is_testing=False):
        self.connection = db_conn
        self.is_testing = is_testing
        self.pg_temp_table = None

    # ASSUMPTION: expectation is to use this class as a context manager, in a with statement
    def __enter__(self):
        if self.is_testing:
            cursor = self.connection.cursor()
            cursor.execute("""SET search_path TO pg_temp""")
            create_tables(self.connection)
            cursor = self.connection.cursor()
            # for testing only, get the real name of the pg_temp table that the test is writing to
            cursor.execute("SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema()")
            self.pg_temp_table = cursor.fetchone()[0]
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            # return False in order to let exception pass through
            return False
        if self.is_testing:
            self.connection.close()
        return True

    def handle_equip_class(self, type, ec_id, depend_ec_ids=None):
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO equip_class(ec_id,type) VALUES(%s, %s) """, (ec_id, type))
        if depend_ec_ids is not None:
            for depend in depend_ec_ids:
                cursor.execute(""" INSERT INTO equip_class_depends(ec_id,ec_id_parent,depend_type) VALUES(%s, %s, %s) """,
                               (ec_id, depend, EC_DEPEND_TYPE_DEFINED))
        self.connection.commit()

    def handle_prod_class(self, type, pc_id, depend_ec_ids=None):
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO prod_class(pc_id,type) VALUES(%s, %s) """, (pc_id, type))
        if depend_ec_ids is not None:
            for depend in depend_ec_ids:
                cursor.execute(""" INSERT INTO prod_class_depends(pc_id,ec_id_parent,depend_type) VALUES(%s, %s, %s)""",
                               (pc_id, depend, EC_DEPEND_TYPE_DEFINED))
        self.connection.commit()

    def handle_store_class(self, type, sc_id):
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO store_class(type, sc_id) VALUES(%s, %s) """, (type, sc_id))
        self.connection.commit()

    def handle_store(self, type, s_id, sc_id):
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO store(type, sc_id, s_id) VALUES(%s, %s, %s) """, (type, sc_id, s_id))
        self.connection.commit()

