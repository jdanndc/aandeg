import traceback
from aandeg.util import create_connection

EC_DEPEND_TYPE_DEFINED = "defined"
EC_DEPEND_TYPE_IMPUTED = "imputed"
INCIDENT_REPORT_TYPE_FAIL = "FAIL"

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
        self.store_class_prod_table_name = 'store_class_prod'
        self.incident_report_table_name = 'incident_report'
        self.drop_equip_table_on_exit = False
        if table_suffix:
            self.equip_class_table_name = self.equip_class_table_name + table_suffix
            self.equip_class_depends_table_name = self.equip_class_depends_table_name + table_suffix
            self.prod_class_table_name = self.prod_class_table_name + table_suffix
            self.prod_class_depends_table_name = self.prod_class_depends_table_name + table_suffix
            self.store_class_table_name = self.store_class_table_name + table_suffix
            self.store_table_name = self.store_table_name + table_suffix
            self.store_class_prod_table_name = self.store_class_prod_table_name + table_suffix
            self.incident_report_table_name = self.incident_report_table_name + table_suffix
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

    def create_store_class_prod_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """ CREATE TABLE {} (
                    sc_id VARCHAR(128) NOT NULL,
                    pc_id VARCHAR(128) NOT NULL
                    )
            """.format(self.store_class_prod_table_name))
        cursor.close()
        self.connection.commit()

    def create_incident_report_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """ CREATE TABLE {} (
                    id SERIAL PRIMARY KEY,
                    s_id VARCHAR(128) NOT NULL,
                    ec_id VARCHAR(128) NOT NULL,
                    type VARCHAR(32) NOT NULL,
                    description VARCHAR(256) NOT NULL
                    )
            """.format(self.incident_report_table_name))
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
        if not self.table_exists(self.store_class_prod_table_name):
            self.create_store_class_prod_table()
        if not self.table_exists(self.incident_report_table_name):
            self.create_incident_report_table()
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
                               (ec_id, depend, EC_DEPEND_TYPE_DEFINED))
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
                cursor.execute(""" INSERT INTO {}(pc_id,ec_id_parent,depend_type) VALUES(%s, %s, %s) """.format(self.prod_class_depends_table_name), (pc_id, depend, EC_DEPEND_TYPE_DEFINED))
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
        cursor.execute("""DELETE FROM {} WHERE depend_type = '{}'""".format(self.equip_class_depends_table_name, EC_DEPEND_TYPE_IMPUTED))
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
                           (dp[0], dp[1], EC_DEPEND_TYPE_IMPUTED, dp[0], dp[1]))
        self.connection.commit()
        cursor.close()

    def update_store_class_default_with_all_prod(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT DISTINCT sc_id FROM {}""".format(self.store_class_table_name))
        for sc in cursor.fetchall():
            cursor.execute(
                """INSERT INTO {} (sc_id, pc_id) SELECT %s, pc_id from {}"""
                    .format(self.store_class_prod_table_name, self.prod_class_table_name), (sc,))
        self.connection.commit()
        cursor.close()

    def create_incident_report(self, s_id, ec_id, itype=INCIDENT_REPORT_TYPE_FAIL, description=""):
        cursor = self.connection.cursor()
        cursor.execute(
            """INSERT INTO {} (s_id, ec_id, type, description) VALUES(%s, %s, %s, %s) RETURNING id"""
                    .format(self.incident_report_table_name), (s_id, ec_id, itype, description))
        id_of_new_row = cursor.fetchone()[0]
        self.connection.commit()
        cursor.close()
        return id_of_new_row

    def clear_store_incidents(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute(
            """DELETE FROM {} WHERE s_id = %s""".format(self.incident_report_table_name), (s_id,))
        self.connection.commit()
        cursor.close()

    def clear_incident(self, i_id):
        cursor = self.connection.cursor()
        cursor.execute(
            """DELETE FROM {} WHERE id = %s""".format(self.incident_report_table_name), (i_id,))
        self.connection.commit()
        cursor.close()

    def get_all_store_products(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute("""
            select  scp.pc_id 
            from {} scp 
            inner join {} s 
            on scp.sc_id=s.sc_id and ( s.s_id = %s)
        """.format(self.store_class_prod_table_name, self.store_table_name), (s_id,))
        self.connection.commit()
        return cursor.fetchall()


    def get_unavailable_store_products(self, s_id):
        if not self.store_is_open(s_id):
            return self.get_all_store_products(s_id)
        cursor = self.connection.cursor()
        cursor.execute("""
        select distinct pcd.pc_id from {} pcd 
        where pcd.ec_id_parent in (
        select ecp.ec_id 
        from {} ecp 
        where ec_id_parent
        in (select ec_id from {} ir where ir.s_id = %s))
            """.format(self.prod_class_depends_table_name, self.equip_class_depends_table_name, self.incident_report_table_name),
                       (s_id,))
        return cursor.fetchall()

    def get_available_store_products(self, s_id):
        if not self.store_is_open(s_id):
            return []
        cursor = self.connection.cursor()
        cursor.execute("""
        select  scp.pc_id 
        from {} scp 
        inner join {} s 
        on scp.sc_id=s.sc_id and ( s.s_id = %s)
        where scp.pc_id not in 
        (
        select distinct pcd.pc_id from {} pcd 
        where pcd.ec_id_parent  in (
        select ecp.ec_id 
        from {} ecp 
        where ec_id_parent 
        in (select ec_id from {} ir where ir.s_id = %s)))
            """.format(
            self.store_class_prod_table_name,
            self.store_table_name,
            self.prod_class_depends_table_name,
            self.equip_class_depends_table_name,
            self.incident_report_table_name),
        (s_id,s_id))
        return cursor.fetchall()

    def store_is_open(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute("""
        /* returns 1 if store is closed, 0 if open */
        select count(distinct ecp.ec_id)
        from {} ecp 
        where ecp.ec_id_parent 
        in (select ec_id from {} ir where ir.s_id = %s)
        and ecp.ec_id = 'store-open'
        """.format(self.equip_class_depends_table_name, self.incident_report_table_name), (s_id,))
        ret = cursor.fetchone()[0]
        return ret == 0

    def drop_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE {}".format(self.equip_class_table_name))
        cursor.execute("DROP TABLE {}".format(self.equip_class_depends_table_name))
        cursor.execute("DROP TABLE {}".format(self.prod_class_table_name))
        cursor.execute("DROP TABLE {}".format(self.prod_class_depends_table_name))
        cursor.execute("DROP TABLE {}".format(self.store_class_table_name))
        cursor.execute("DROP TABLE {}".format(self.store_table_name))
        cursor.execute("DROP TABLE {}".format(self.store_class_prod_table_name))
        cursor.execute("DROP TABLE {}".format(self.incident_report_table_name))
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

