import db_connection as db


class create_m_clients:

    def __init__(self):
        try:
            self.db_cnt = db.db_connection().get_connection()
            self.db_cursor = self.db_cnt.cursor()
        except:
            print('create_m_clients init except')

    def execute(self, maching_data_id):
        try:
            self.db_cnt.autocommit(False)

            repair_data = self.__get_repair_data(maching_data_id)
            for item in repair_data:
                client_code = item['client_code'].strip()
                client_name = item['client_name'].strip()
                client_name_kana = item['client_name_kana'].strip()
                department_code = item['department_code'].strip()
                billing_destination_code = item['billing_destination_code'].strip(
                )

                if (self.__is_client_code_already_exist(client_code)):
                    m_billing_destination_id = self.__get_m_billing_destination_id(
                        department_code, billing_destination_code)
                    if (m_billing_destination_id is False):
                        raise Exception(
                            '[create_m_clients - ERROR] [update] m_billing_destination_id were not find.')
                    self.__update_m_clients(
                        m_billing_destination_id, client_code, client_name, client_name_kana)
                else:
                    m_billing_destination_id = self.__get_m_billing_destination_id(
                        department_code, billing_destination_code)
                    if (m_billing_destination_id is not False):
                        raise Exception(
                            '[create_m_clients - ERROR] [create] m_billing_destination_id were not find.')
                    self.__create_m_clients(
                        m_billing_destination_id, client_code, client_name, client_name_kana)

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
                    `client_code`, \
                    `client_name`, \
                    `client_name_kana`, \
                    `department_code`, \
                    `billing_destination_code` \
                FROM \
	                `t_repair_data` \
                WHERE \
                    `t_matching_data_id` = {maching_data_id} \
                    AND `deleted_at` IS NULL \
                GROUP BY \
                    `client_code`, \
                    `client_name`, \
                    `client_name_kana`, \
                    `department_code`, \
                    `billing_destination_code` \
                ;"
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def __is_client_code_already_exist(self, client_code):
        query = f"SELECT \
                    `client_code` \
                FROM \
	                `m_clients` \
                WHERE \
                    `client_code` = '{client_code}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        if (self.db_cursor.fetchone() is None):
            return False
        return True

    def __get_m_billing_destination_id(self, department_code, billing_destination_code):
        query = f"SELECT \
                    `id` \
                FROM \
	                `m_billing_destinations` \
                WHERE \
                    `department_code` = '{department_code}' \
                    AND `billing_destination_code` = '{billing_destination_code}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        data = self.db_cursor.fetchone()
        if (data is None):
            return False
        return data['id']

    def __update_m_clients(self, m_billing_destination_id, client_code, client_name, client_name_kana):
        query = f"UPDATE \
	                `m_clients` \
                SET \
                    `m_billing_destination_id` = {m_billing_destination_id}, \
                    `client_name` = '{client_name}', \
                    `client_name_kana` = '{client_name_kana}' \
                WHERE \
                    `client_code` = '{client_code}' \
                ;"
        self.db_cursor.execute(query)

    def __create_m_clients(self, m_billing_destination_id, client_code, client_name, client_name_kana):
        query = f"INSERT INTO \
	                `m_clients` (`m_billing_destination_id`, `client_code`,`client_name`,`client_name_kana`) \
                VALUES \
                    ({m_billing_destination_id}, '{client_code}', '{client_name}', '{client_name_kana}') \
                ;"
        self.db_cursor.execute(query)
