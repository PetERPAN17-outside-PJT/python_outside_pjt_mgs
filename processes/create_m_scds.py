import db_connection as db


class create_m_scds:

    def __init__(self):
        try:
            self.db_cnt = db.db_connection().get_connection()
            self.db_cursor = self.db_cnt.cursor()
        except:
            print('create_m_scds init except')

    def execute(self, maching_data_id):
        try:
            self.db_cnt.autocommit(False)

            repair_data = self.__get_repair_data(maching_data_id)
            for item in repair_data:
                if (item['management_number'] is None):
                    continue

                if (item['scd'] is None):
                    continue

                management_number = item['management_number'].strip()
                scd = item['scd'].strip()

                if (self.__is_data_already_exist(management_number, scd) is False):
                    self.__create_m_scds(management_number, scd)

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
                    `management_number`, \
                    `scd` \
                FROM \
	                `t_repair_data` \
                WHERE \
                    `t_matching_data_id` = {maching_data_id} \
                    AND `deleted_at` IS NULL \
                GROUP BY \
                    `management_number`, \
                    `scd` \
                ;"
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def __is_data_already_exist(self, management_number, scd):
        query = f"SELECT \
                    `id` \
                FROM \
	                `m_scds` \
                WHERE \
                    `management_number` = '{management_number}' \
                    AND `scd` = '{scd}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        if (self.db_cursor.fetchone() is None):
            return False
        return True

    def __create_m_scds(self, management_number, scd):
        query = f"INSERT INTO \
	                `m_scds` (`management_number`, `scd`) \
                VALUES \
                    ('{management_number}', '{scd}') \
                ;"
        self.db_cursor.execute(query)
