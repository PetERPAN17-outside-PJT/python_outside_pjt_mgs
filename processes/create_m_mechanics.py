import db_connection as db
from processes.create_m_mechanic_belongs_to import create_m_mechanic_belongs_to


class create_m_mechanics:

    def __init__(self):
        try:
            self.db_cnt = db.db_connection().get_connection()
            self.db_cursor = self.db_cnt.cursor()
        except:
            print('create_m_mechanics init except')

    def execute(self, maching_data_id):
        try:
            self.db_cnt.autocommit(False)

            repair_data = self.__get_repair_data(maching_data_id)
            for item in repair_data:
                mechanic = item['mechanic'].strip()
                inside_outside = item['inside_outside'].strip()
                department_code = item['department_code'].strip()
                billing_destination_code = item['billing_destination_code'].strip()
                client_code = item['client_code'].strip()

                m_mechanic_id = self.__get_m_mechanic_id(mechanic)
                if (m_mechanic_id):
                    m_inside_outside_id = self.__get_m_inside_outside_id(
                        inside_outside)
                    if (m_inside_outside_id is False):
                        raise Exception(
                            '[create_m_mechanics - ERROR] m_inside_outside_id were not find.')

                    self.__update_m_mechanics(m_inside_outside_id, mechanic)
                    create_m_mechanic_belongs_to(self.db_cnt, self.db_cursor).update(
                        m_mechanic_id, department_code, billing_destination_code, client_code)
                else:
                    m_inside_outside_id = self.__get_m_inside_outside_id(
                        inside_outside)
                    if (m_inside_outside_id is False):
                        raise Exception(
                            '[create_m_mechanics - ERROR] m_inside_outside_id were not find.')

                    self.__create_m_mechanics(m_inside_outside_id, mechanic)
                    create_m_mechanic_belongs_to(self.db_cnt, self.db_cursor).create(
                        self.db_cursor.lastrowid, department_code, billing_destination_code, client_code)

            self.db_cnt.commit()
            return True
        except Exception as e:
            self.db_cnt.rollback()
            print(e)
            return e
        finally:
            self.db_cnt.close()

    def __get_repair_data(self, maching_data_id):
        query = f"SELECT \
                    `mechanic`, \
                    `inside_outside`, \
                    `department_code`, \
                    `billing_destination_code`, \
                    `client_code` \
                FROM \
	                `t_repair_data` \
                WHERE \
                    `t_matching_data_id` = {maching_data_id} \
                    AND `deleted_at` IS NULL \
                ORDER BY \
                    `id` ASC \
                ;"
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def __get_m_mechanic_id(self, mechanic):
        query = f"SELECT \
                    `id`, \
                    `mechanic_name` \
                FROM \
	                `m_mechanics` \
                WHERE \
                    `mechanic_name` = '{mechanic}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        data = self.db_cursor.fetchone()
        if (data is None):
            return False
        return data['id']

    def __get_m_inside_outside_id(self, inside_outside):
        query = f"SELECT \
                    `id` \
                FROM \
	                `m_inside_outsides` \
                WHERE \
                    `inside_outside` = '{inside_outside}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        data = self.db_cursor.fetchone()
        if (data is None):
            return False
        return data['id']

    def __update_m_mechanics(self, m_inside_outside_id, mechanic):
        query = f"UPDATE \
	                `m_mechanics` \
                SET \
                    `m_inside_outside_id` = {m_inside_outside_id} \
                WHERE \
                    `mechanic_name` = '{mechanic}' \
                ;"
        self.db_cursor.execute(query)

    def __create_m_mechanics(self, m_inside_outside_id, mechanic):
        query = f"INSERT INTO \
	                `m_mechanics` (`m_inside_outside_id`, `mechanic_name`) \
                VALUES \
                    ({m_inside_outside_id}, '{mechanic}') \
                ;"
        self.db_cursor.execute(query)
