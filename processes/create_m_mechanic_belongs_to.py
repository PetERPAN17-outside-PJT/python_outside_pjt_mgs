class create_m_mechanic_belongs_to:

    def __init__(self, db_cnt, db_cursor):
        self.db_cnt = db_cnt
        self.db_cursor = db_cursor

    def update(self, m_mechanic_id, department_code, billing_destination_code, client_code):
        m_billing_destination_id = self.__get_m_billing_destination_id(
            department_code, billing_destination_code)
        if (m_billing_destination_id is False):
            raise Exception(
                '[create_m_mechanic_belongs_to - ERROR] [update] m_billing_destination_id were not find.')

        m_client_id = self.__get_m_client_id(client_code)
        if (m_client_id is False):
            raise Exception(
                '[create_m_mechanic_belongs_to - ERROR] [update] m_client_id were not find.')

        self.__update_m_mechanic_belongs_to(
            m_mechanic_id, m_billing_destination_id, m_client_id)

    def create(self, m_mechanic_id, department_code, billing_destination_code, client_code):
        m_billing_destination_id = self.__get_m_billing_destination_id(
            department_code, billing_destination_code)
        if (m_billing_destination_id is False):
            raise Exception(
                '[create_m_mechanic_belongs_to - ERROR] [create] m_billing_destination_id were not find.')

        m_client_id = self.__get_m_client_id(client_code)
        if (m_client_id is False):
            raise Exception(
                '[create_m_mechanic_belongs_to - ERROR] [create] m_client_id were not find.')

        self.__create_m_mechanic_belongs_to(
            m_mechanic_id, m_billing_destination_id, m_client_id)

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

    def __get_m_client_id(self, client_code):
        query = f"SELECT \
                    `id` \
                FROM \
	                `m_clients` \
                WHERE \
                    `client_code` = '{client_code}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        data = self.db_cursor.fetchone()
        if (data is None):
            return False
        return data['id']

    def __update_m_mechanic_belongs_to(self, m_mechanic_id, m_billing_destination_id, m_client_id):
        query = f"UPDATE \
	                `m_mechanic_belongs_to` \
                SET \
                    `m_billing_destination_id` = {m_billing_destination_id}, \
                    `m_client_id` = {m_client_id} \
                WHERE \
                    `m_mechanic_id` = '{m_mechanic_id}' \
                ;"
        self.db_cursor.execute(query)

    def __create_m_mechanic_belongs_to(self, m_mechanic_id, m_billing_destination_id, m_client_id):
        query = f"INSERT INTO \
	                `m_mechanic_belongs_to` (`m_mechanic_id`, `m_billing_destination_id`, `m_client_id`) \
                VALUES \
                    ({m_mechanic_id}, {m_billing_destination_id}, {m_client_id}) \
                ;"
        self.db_cursor.execute(query)
