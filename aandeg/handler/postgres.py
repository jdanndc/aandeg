import traceback
from aandeg.util import create_connection

from handler.base import BaseHandler
from handler.base import EC_DEPEND_TYPE_DEFINED, EC_DEPEND_TYPE_IMPUTED, INCIDENT_REPORT_TYPE_FAIL,  STORE_OPEN_EQUIP_ID


# TODO:
# create a class RDBHandler(BaseHandler)
# that handles all the common RDB functionality between AWS RDS and local Postgres
# and then one instance for PostgresHandler and one for AWS RDS
class PostgresHandler(BaseHandler):
    # TODO: handle the password more safely
    def __init__(self, db_name, db_user, db_password, db_host, db_port, is_testing=False):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.connection = None
        self.is_testing = is_testing
        self.pg_temp_table = None

    def create_tables(self):
        cursor = self.connection.cursor()
        cursor.execute(open('data/schema_create.sql', 'r').read())
        cursor.execute("SHOW search_path")
        sp = cursor.fetchone()[0]
        self.connection.commit()

    # ASSUMPTION: expectation is to use this class as a context manager, in a with statement
    def __enter__(self):
        self.connection = create_connection(self.db_name, self.db_user, self.db_password, self.db_host, self.db_port)
        if self.is_testing:
            cursor = self.connection.cursor()
            cursor.execute("""SET search_path TO pg_temp""")
        self.create_tables()
        if self.is_testing:
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

    def _add_depend_pairs_to_set(self, ec_id, all_depend_pairs, add_pair_for_self=True):
        # get all ancestors of this ec_id, using CTE query
        cursor = self.connection.cursor()
        cursor.execute("""
            WITH RECURSIVE parents AS (
        	    SELECT ec_id, ec_id_parent
        	    FROM equip_class_depends
        	    WHERE ec_id = %s 
            	UNION SELECT e.ec_id, e.ec_id_parent FROM equip_class_depends e
            		INNER JOIN parents p ON p.ec_id_parent = e.ec_id
            ) SELECT * FROM parents;
        	""", (ec_id,))
        for row in cursor:
            all_depend_pairs.add((ec_id, row[1]))
        if add_pair_for_self:
            all_depend_pairs.add((ec_id, ec_id))

    def update_imputed_depends(self):
        cursor = self.connection.cursor()
        cursor.execute("""DELETE FROM equip_class_depends WHERE depend_type = '{}'""".format(EC_DEPEND_TYPE_IMPUTED))
        # TODO: this is here for stepping thru and watching in DB, delete later
        self.connection.commit()
        # TODO: consider the case where an ec_id is in the equip_class_depends table, but not in the equip_class table
        cursor.execute("""SELECT DISTINCT ec_id FROM equip_class""")
        distinct_equip_types = []
        for row in cursor:
            distinct_equip_types.append(row[0])
        # create a set of tuples of each depend pair
        all_depend_pairs = set()
        for ec_id in distinct_equip_types:
            self._add_depend_pairs_to_set(ec_id, all_depend_pairs)
        for dp in all_depend_pairs:
            print(dp)
            cursor.execute("""
            INSERT INTO equip_class_depends (ec_id, ec_id_parent, depend_type)
            SELECT %s, %s, %s
            WHERE
                NOT EXISTS (
                    SELECT ec_id FROM equip_class_depends WHERE ec_id = %s and ec_id_parent = %s
                )
            """, (dp[0], dp[1], EC_DEPEND_TYPE_IMPUTED, dp[0], dp[1]))
        self.connection.commit()
        cursor.close()

    def create_incident_report(self, s_id, ec_id, itype=INCIDENT_REPORT_TYPE_FAIL, description=""):
        # TODO: add a trigger, for now, just check values with a select
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO incident_report(s_id, ec_id, type, description) 
            SELECT 
                (SELECT s_id FROM store WHERE s_id = %s),
                (SELECT ec_id FROM equip_class WHERE ec_id = %s),
                %s, %s  
            RETURNING ID
            """, (s_id, ec_id, itype, description)
        )
        id_of_new_row = None
        ff = cursor.fetchone()
        if len(ff) > 0:
            id_of_new_row = ff[0]
        self.connection.commit()
        cursor.close()
        return id_of_new_row

    def clear_store_incidents(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute( """DELETE FROM incident_report WHERE s_id = %s""", (s_id,))
        self.connection.commit()
        cursor.close()

    def clear_incident(self, i_id):
        cursor = self.connection.cursor()
        cursor.execute( """DELETE FROM incident_report WHERE id = %s""", (i_id,))
        self.connection.commit()
        cursor.close()

    def get_store_incidents(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute( """SELECT * FROM incident_report WHERE s_id = %s""", (s_id,))
        return cursor.fetchall()

    def list_table(self, table_name):
        # TODO: beware SQL injection here
        cursor = self.connection.cursor()
        cursor.execute( """SELECT * FROM {}""".format(table_name))
        return cursor.fetchall()

    def get_all_store_products(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT scp.pc_id 
            FROM store_class_prod scp 
            INNER JOIN store s 
            ON scp.sc_id=s.sc_id AND (s.s_id = %s)
        """, (s_id,))
        self.connection.commit()
        return cursor.fetchall()

    def get_unavailable_store_products(self, s_id):
        if not self.store_is_open(s_id):
            return self.get_all_store_products(s_id)
        cursor = self.connection.cursor()
        cursor.execute("""
        SELECT DISTINCT pcd.pc_id FROM prod_class_depends pcd 
        WHERE pcd.ec_id_parent IN (
            SELECT ecp.ec_id 
            FROM equip_class_depends ecp 
            WHERE ec_id_parent IN (SELECT ec_id FROM incident_report ir WHERE ir.s_id = %s)
        )
        """, (s_id,))
        return cursor.fetchall()

    def get_available_store_products(self, s_id):
        if not self.store_is_open(s_id):
            return []
        cursor = self.connection.cursor()
        cursor.execute("""
        SELECT scp.pc_id 
        FROM store_class_prod scp 
        INNER JOIN store s 
        ON scp.sc_id=s.sc_id AND ( s.s_id = %s)
        WHERE scp.pc_id NOT IN 
        (
        SELECT DISTINCT pcd.pc_id FROM prod_class_depends pcd 
        WHERE pcd.ec_id_parent IN (
        SELECT ecp.ec_id 
        FROM equip_class_depends ecp 
        WHERE ec_id_parent 
        IN (SELECT ec_id FROM incident_report ir WHERE ir.s_id = %s)))
            """, (s_id,s_id))
        return cursor.fetchall()

    def store_is_open(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute("""
        /* returns 1 if store is closed, 0 if open */
        SELECT COUNT(DISTINCT ecp.ec_id)
        FROM equip_class_depends ecp 
        WHERE ecp.ec_id_parent 
            IN (SELECT ec_id FROM incident_report ir WHERE ir.s_id = %s)
            AND ecp.ec_id = '{}'
        """.format(STORE_OPEN_EQUIP_ID), (s_id,))
        ret = cursor.fetchone()[0]
        return ret == 0

    def drop_tables(self):
        cursor = self.connection.cursor()
        cursor.execute(open('./data/schema_drop.sql', 'r').read())
        self.connection.commit()
        return self

    # drops all tables in the schema
    def drop_all_tables(self):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
        rows = cursor.fetchall()
        for row in rows:
            cursor.execute("DROP TABLE " + row[1] + " CASCADE")
        self.connection.commit()
        cursor.close()

    # TODO: this is only for demo purposes
    # all store classes get all products
    # in a real system, different store classes would have different sets of products
    # and these relations would be filled in through data
    def update_store_classes_with_all_products(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT DISTINCT sc_id FROM store_class""")
        for sc in cursor.fetchall():
            cursor.execute("""INSERT INTO store_class_prod(sc_id, pc_id) SELECT %s, pc_id from prod_class""", (sc,))
        self.connection.commit()
        cursor.close()


