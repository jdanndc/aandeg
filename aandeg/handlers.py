import traceback
from aandeg.aandeg_util import create_connection


class PostgresHandler(object):
    # TODO: handle the password more safely
    def __init__(self, db_name, db_user, db_password, db_host, db_port, drop_on_exit=False, table_suffix=None):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.equip_class_table_name = 'equip_class'
        self.equip_class_depends_table_name = 'equip_class_depends'
        self.prod_class_table_name = 'prod_class'
        self.prod_class_depends_table_name = 'prod_class_depends'
        self.store_class_table_name = 'store_class'
        self.store_table_name = 'store'
        self.drop_equip_table_on_exit = False
        if table_suffix:
            self.equip_class_table_name = self.equip_class_table_name + table_suffix
            self.equip_class_depends_table_name = self.equip_class_depends_table_name + table_suffix
            self.prod_class_table_name = self.prod_class_table_name + table_suffix
            self.prod_class_depends_table_name = self.prod_class_depends_table_name + table_suffix
            self.store_class_table_name = self.store_class_table_name + table_suffix
            self.store_table_name = self.store_table_name + table_suffix
        self.connection = None

    def table_exists(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
        b = cursor.fetchone()[0]
        return b

    def create_equip_class_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """ CREATE TABLE {} (
                    ec_id VARCHAR(128) PRIMARY KEY,
                    type VARCHAR(128) NOT NULL
                    )
            """.format(self.equip_class_table_name))
        cursor.close()
        self.connection.commit()

    def create_equip_class_depends_table(self):
        cursor = self.connection.cursor()
        # TODO:
        #  CREATE TYPE valid_depend_types AS ENUM ('defined', 'imputed');
        #  CREATE TABLE t (ec_id..., ec_id_parent..., depend_type VALID_DEPEND_TYPES);
        cursor.execute(
            """ CREATE TABLE {} (
                    ec_id VARCHAR(128) NOT NULL,
                    ec_id_parent VARCHAR(128) NOT NULL,
                    depend_type VARCHAR(32) NOT NULL
                    )
            """.format(self.equip_class_depends_table_name))
        cursor.close()
        self.connection.commit()

    def create_prod_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """ CREATE TABLE {} (
                    pc_id VARCHAR(128) PRIMARY KEY,
                    type VARCHAR(32) NOT NULL
                    )
            """.format(self.prod_class_table_name))
        cursor.close()
        self.connection.commit()

    def create_prod_class_depends_table(self):
        cursor = self.connection.cursor()
        # TODO:
        #  CREATE TYPE valid_depend_types AS ENUM ('defined', 'imputed');
        #  CREATE TABLE t (pc_id..., ec_id_parent..., depend_type VALID_DEPEND_TYPES);
        cursor.execute(
            """ CREATE TABLE {} (
                    pc_id VARCHAR(128) NOT NULL,
                    ec_id_parent VARCHAR(128) NOT NULL,
                    depend_type VARCHAR(32) NOT NULL
                    )
            """.format(self.prod_class_depends_table_name))
        cursor.close()
        self.connection.commit()

    def create_store_class_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """ CREATE TABLE {} (
                    sc_id VARCHAR(128) NOT NULL,
                    type VARCHAR(32) NOT NULL
                    )
            """.format(self.store_class_table_name))
        cursor.close()
        self.connection.commit()

    def create_store_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """ CREATE TABLE {} (
                    s_id VARCHAR(128) NOT NULL,
                    sc_id VARCHAR(128) NOT NULL,
                    type VARCHAR(32) NOT NULL
                    )
            """.format(self.store_table_name))
        cursor.close()
        self.connection.commit()

    def __enter__(self):
        self.connection = create_connection(self.db_name, self.db_user, self.db_password, self.db_host, self.db_port)
        # create equip table if does not exist
        if not self.table_exists(self.equip_class_table_name):
            self.create_equip_class_table()
        if not self.table_exists(self.equip_class_depends_table_name):
            self.create_equip_class_depends_table()
        if not self.table_exists(self.prod_class_table_name):
            self.create_prod_table()
        if not self.table_exists(self.prod_class_depends_table_name):
            self.create_prod_class_depends_table()
        if not self.table_exists(self.store_class_table_name):
            self.create_store_class_table()
        if not self.table_exists(self.store_table_name):
            self.create_store_table()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            return False
            # return False in order to let exception pass through
        if self.drop_equip_table_on_exit:
            pass
        self.connection.close()
        return True

    def handle_equip_class(self, type, ec_id, depend_ec_ids=None):
        # TODO: check that ec_id_parent exists before inserting into the depends table
        #  we do check while reading the file, but the ec_id of a ec_id_parent could be deleted out-of-band
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO {}(ec_id,type) VALUES(%s, %s) """.format(self.equip_class_table_name), (ec_id, type))
        if depend_ec_ids is not None:
            for depend in depend_ec_ids:
                # TODO: confirm that the ec_id for depend exists in equip_class table
                #  was previously checked during json parsing, but disabled to allow for update
                cursor.execute(""" INSERT INTO {}(ec_id,ec_id_parent,depend_type) VALUES(%s, %s, %s) """.format(self.equip_class_depends_table_name),
                               (ec_id, depend, 'defined'))
        self.connection.commit()

    def handle_prod_class(self, type, pc_id, depend_ec_ids=None):
        # todo: check that ec_id exists before inserting into the depends table
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO {}(pc_id,type) VALUES(%s, %s) """.format(self.prod_class_table_name), (pc_id, type))
        if depend_ec_ids is not None:
            for depend in depend_ec_ids:
                # todo: confirm that the ec_id for depend exists in equip_class table
                #  was previously checked during json parsing, but disabled to allow for update
                pass
                cursor.execute(""" INSERT INTO {}(pc_id,ec_id_parent,depend_type) VALUES(%s, %s, %s) """.format(self.prod_class_depends_table_name), (pc_id, depend, 'defined'))
        self.connection.commit()

    def handle_store_class(self, type, sc_id):
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO {}(type, sc_id) VALUES(%s, %s) """.format(self.store_class_table_name), (type, sc_id))
        self.connection.commit()

    def handle_store(self, type, s_id, sc_id):
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO {}(type, sc_id, s_id) VALUES(%s, %s, %s) """.format(self.store_table_name), (type, sc_id, s_id))
        self.connection.commit()

    def _add_depend_pairs_to_set(self, ec_id, all_depend_pairs, add_pair_for_self=True):
        # get all ancestors of this ec_id, using CTE query
        cursor = self.connection.cursor()
        cursor.execute("""
            WITH RECURSIVE parents AS (
        	    SELECT ec_id, ec_id_parent
        	    FROM {}
        	    WHERE ec_id = %s 
            	UNION SELECT e.ec_id, e.ec_id_parent FROM {} e
            		INNER JOIN parents p ON p.ec_id_parent = e.ec_id
            ) SELECT * FROM parents;
        	""".format(self.equip_class_depends_table_name, self.equip_class_depends_table_name), (ec_id,))
        for row in cursor:
            all_depend_pairs.add((ec_id, row[1]))
        if add_pair_for_self:
            all_depend_pairs.add((ec_id, ec_id))

    def update_imputed_depends(self):
        cursor = self.connection.cursor()
        cursor.execute("""DELETE FROM {} WHERE depend_type = 'imputed'""".format(self.equip_class_depends_table_name))
        # TODO: this is here for stepping thru and watching in DB, delete later
        self.connection.commit()
        # TODO: consider the case where an ec_id is in the equip_class_depends table, but not in the equip_class table
        cursor.execute("""SELECT DISTINCT ec_id FROM {}""".format(self.equip_class_table_name))
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
            INSERT INTO {} (ec_id, ec_id_parent, depend_type)
            SELECT %s, %s, %s
            WHERE
                NOT EXISTS (
                    SELECT ec_id FROM {} WHERE ec_id = %s and ec_id_parent = %s
                )
            """.format(self.equip_class_depends_table_name, self.equip_class_depends_table_name),
                           (dp[0], dp[1], 'imputed', dp[0], dp[1]))
        self.connection.commit()
        cursor.close()


    def drop_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE {}".format(self.equip_class_table_name))
        cursor.execute("DROP TABLE {}".format(self.equip_class_depends_table_name))
        cursor.execute("DROP TABLE {}".format(self.prod_class_table_name))
        cursor.execute("DROP TABLE {}".format(self.prod_class_depends_table_name))
        cursor.execute("DROP TABLE {}".format(self.store_class_table_name))
        cursor.execute("DROP TABLE {}".format(self.store_table_name))
        self.connection.commit()
        cursor.close()

    def drop_all_tables(self):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
        rows = cursor.fetchall()
        for row in rows:
            cursor.execute("DROP TABLE " + row[1] + " CASCADE")
        self.connection.commit()
        cursor.close()


class CollectHandler():
    def __init__(self):
        self.equip_class_collect_list = []
        self.prod_class_collect_list = []
        self.store_class_collect_list = []
        self.store_collect_list = []

    def handle_equip_class(self, type, ecid, depend_ecids=None):
        d = {'type':type, 'ecid':ecid, 'depend_ecids':depend_ecids}
        self.equip_class_collect_list.append(d)

    def handle_prod_class(self, type, pc_id, depend_ecids=None):
        d = {'type':type, 'pc_id':pc_id, 'depend_ecids':depend_ecids}
        self.prod_class_collect_list.append(d)

    def handle_store_class(self, type, sc_id):
        d = {'type':type, 'sc_id':sc_id}
        self.store_class_collect_list.append(d)

    def handle_store(self, type, s_id, sc_id):
        d = {'type':type, 's_id':s_id, 'sc_id':sc_id}
        self.store_collect_list.append(d)

