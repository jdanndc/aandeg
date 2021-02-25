import traceback
from aandeg.pgutil import create_connection


class PostgresHandler(object):
    # TODO: handle the password more safely
    def __init__(self, db_name, db_user, db_password, db_host, db_port, drop_on_exit=False, table_suffix=None):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.equip_class_table_name = 'equip_class'
        self.equip_depends_table_name = 'equip_depends'
        self.drop_equip_table_on_exit = False
        if table_suffix:
            self.equip_class_table_name = self.equip_class_table_name + table_suffix
            self.equip_depends_table_name = self.equip_depends_table_name + table_suffix
        self.connection = None

    def table_exists(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
        b = cursor.fetchone()[0]
        return b

    def create_equip_table(self):
        cursor = self.connection.cursor()
        cursor.execute(
            """ CREATE TABLE {} (
                    eid VARCHAR(128) PRIMARY KEY,
                    etype VARCHAR(128) NOT NULL
                    )
            """.format(self.equip_class_table_name))
        cursor.close()
        self.connection.commit()

    def create_equip_depends_table(self):
        cursor = self.connection.cursor()
        # TODO:
        #  CREATE TYPE valid_depend_types AS ENUM ('defined', 'imputed');
        #  CREATE TABLE t (eid..., eid_parent..., depend_type VALID_DEPEND_TYPES);
        cursor.execute(
            """ CREATE TABLE {} (
                    eid VARCHAR(128) NOT NULL,
                    eid_parent VARCHAR(128) NOT NULL,
                    depend_type VARCHAR(8) NOT NULL
                    )
            """.format(self.equip_depends_table_name))
        cursor.close()
        self.connection.commit()

    def __enter__(self):
        self.connection = create_connection(self.db_name, self.db_user, self.db_password, self.db_host, self.db_port)
        # create equip table if does not exist
        if not self.table_exists(self.equip_class_table_name):
            self.create_equip_table()
        if not self.table_exists(self.equip_depends_table_name):
            self.create_equip_depends_table()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            # return False in order to let exception pass through
        if self.drop_equip_table_on_exit:
            pass
        self.connection.close()
        return True

    def handle_equip(self, etype, eid, depend_eids=None):
        # TODO: check that eid_parent exists before inserting into the depends table
        #  we do check while reading the file, but the eid of a eid_parent could be deleted out-of-band
        cursor = self.connection.cursor()
        cursor.execute(""" INSERT INTO {}(eid,etype) VALUES(%s, %s) """.format(self.equip_class_table_name), (eid, etype))
        if depend_eids is not None:
            for depend in depend_eids:
                # TODO: confirm that the eid for depend exists in equip_class table
                #  was previously checked during json parsing, but disabled to allow for update
                cursor.execute(""" INSERT INTO {}(eid,eid_parent,depend_type) VALUES(%s, %s, %s) """.format(self.equip_depends_table_name),
                               (eid, depend, 'defined'))
        self.connection.commit()

    def _add_depend_pairs_to_set(self, eid, all_depend_pairs):
        # get all ancestors of this eid, using CTE query
        cursor = self.connection.cursor()
        cursor.execute("""
            WITH RECURSIVE parents AS (
        	    SELECT eid, eid_parent
        	    FROM {}
        	    WHERE eid = %s 
            	UNION SELECT e.eid, e.eid_parent FROM {} e
            		INNER JOIN parents p ON p.eid_parent = e.eid
            ) SELECT * FROM parents;
        	""".format(self.equip_depends_table_name, self.equip_depends_table_name), (eid,))
        for row in cursor:
            all_depend_pairs.add((eid, row[1]))

    def update_imputed_depends(self):
        cursor = self.connection.cursor()
        cursor.execute("""DELETE FROM {} WHERE depend_type = 'imputed'""".format(self.equip_depends_table_name))
        # TODO: this is here for stepping thru and watching in DB, delete later
        self.connection.commit()
        # TODO: consider the case where an eid is in the equip_depends table, but not in the equip_class table
        cursor.execute("""SELECT DISTINCT eid FROM {}""".format(self.equip_class_table_name))
        distinct_equip_types = []
        for row in cursor:
            distinct_equip_types.append(row[0])
        # create a set of tuples of each depend pair
        all_depend_pairs = set()
        for eid in distinct_equip_types:
            self._add_depend_pairs_to_set(eid, all_depend_pairs)
        for dp in all_depend_pairs:
            print(dp)
            cursor.execute("""
            INSERT INTO {} (eid, eid_parent, depend_type)
            SELECT %s, %s, %s
            WHERE
                NOT EXISTS (
                    SELECT eid FROM {} WHERE eid = %s and eid_parent = %s
                )
            """.format(self.equip_depends_table_name, self.equip_depends_table_name),
                           (dp[0], dp[1], 'imputed', dp[0], dp[1]))
        self.connection.commit()
        cursor.close()


    def drop_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE {}".format(self.equip_class_table_name))
        cursor.execute("DROP TABLE {}".format(self.equip_depends_table_name))
        self.connection.commit()
        cursor.close()



class PrintHandler():
    def __init__(self):
        pass

    def handle_equip(self, etype, eid, depend_eids=None):
        print("eid:{}, etype:{}, depends:{}".format(etype, eid, ",".join(depend_eids)))


class CollectHandler():
    def __init__(self):
        self.collect_list = []

    def handle_equip(self, etype, eid, depend_eids=None):
        d = {'etype':etype, 'eid':eid, 'depend_eids':depend_eids}
        self.collect_list.append(d)

