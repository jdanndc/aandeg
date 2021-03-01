from aandeg.data_handler.base import EC_DEPEND_TYPE_IMPUTED, INCIDENT_REPORT_TYPE_FAIL,  STORE_OPEN_EQUIP_ID


class Model():
    def __init__(self, db_conn):
        self.connection = db_conn
        pass

    # return id of new incident report
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
            """, (s_id, ec_id, itype, description))
        id_of_new_row = None
        ff = cursor.fetchone()
        if len(ff) > 0:
            id_of_new_row = ff[0]
        self.connection.commit()
        cursor.close()
        return id_of_new_row

    def clear_all_incidents(self):
        cursor = self.connection.cursor()
        cursor.execute("""DELETE FROM incident_report""")
        self.connection.commit()
        cursor.close()

    def clear_store_incidents(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute("""DELETE FROM incident_report WHERE s_id = %s""", (s_id,))
        self.connection.commit()
        cursor.close()

    def clear_incident(self, i_id):
        cursor = self.connection.cursor()
        cursor.execute("""DELETE FROM incident_report WHERE id = %s""", (i_id,))
        self.connection.commit()
        cursor.close()

    def get_store_incidents(self, s_id):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT * FROM incident_report WHERE s_id = %s""", (s_id,))
        return cursor.fetchall()

    def get_incidents(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT * FROM incident_report""")
        return cursor.fetchall()

    def list_table(self, table_name):
        # TODO: beware SQL injection here
        cursor = self.connection.cursor()
        cursor.execute("""SELECT * FROM {}""".format(table_name))
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
            """, (s_id, s_id))
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
        return (0 == cursor.fetchone()[0])

    # return count of pairs added
    def add_depend_pairs_to_set(self, ec_id, all_depend_pairs, add_pair_for_self=True):
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
        count = 0
        for row in cursor:
            all_depend_pairs.add((ec_id, row[1]))
            count = count + 1
        if add_pair_for_self:
            all_depend_pairs.add((ec_id, ec_id))
            count = count + 1
        return count


    # return number of imputed depends added
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
            self.add_depend_pairs_to_set(ec_id, all_depend_pairs)
        count = 0
        for dp in all_depend_pairs:
            cursor.execute("""
            INSERT INTO equip_class_depends (ec_id, ec_id_parent, depend_type)
            SELECT %s, %s, %s
            WHERE
                NOT EXISTS (
                    SELECT ec_id FROM equip_class_depends WHERE ec_id = %s and ec_id_parent = %s
                )
            """, (dp[0], dp[1], EC_DEPEND_TYPE_IMPUTED, dp[0], dp[1]))
            count = count + 1
        self.connection.commit()
        cursor.close()
        return count

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


