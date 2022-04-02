import db_connection as db


class create_m_vehicle_manufacturers:

    def __init__(self):
        try:
            self.db_cnt = db.db_connection().get_connection()
            self.db_cursor = self.db_cnt.cursor()
        except:
            print('create_m_vehicle_manufacturers init except')

    def execute(self, maching_data_id):
        try:
            self.db_cnt.autocommit(False)

            repair_data = self.__get_repair_data(maching_data_id)
            for item in repair_data:
                if (item['vehicle_manufacturer'] is None):
                    continue

                vehicle_manufacturer = item['vehicle_manufacturer'].strip()

                if (self.__is_vehicle_manufacturer_already_exist(vehicle_manufacturer) is False):
                    self.__create_m_vehicle_manufacturers(vehicle_manufacturer)

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
                    `vehicle_manufacturer` \
                FROM \
	                `t_repair_data` \
                WHERE \
                    `t_matching_data_id` = {maching_data_id} \
                    AND `deleted_at` IS NULL \
                GROUP BY \
                    `vehicle_manufacturer` \
                ;"
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def __is_vehicle_manufacturer_already_exist(self, vehicle_manufacturer):
        query = f"SELECT \
                    `vehicle_manufacturer_name` \
                FROM \
	                `m_vehicle_manufacturers` \
                WHERE \
                    `vehicle_manufacturer_name` = '{vehicle_manufacturer}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        if (self.db_cursor.fetchone() is None):
            return False
        return True

    def __create_m_vehicle_manufacturers(self, vehicle_manufacturer):
        query = f"INSERT INTO \
	                `m_vehicle_manufacturers` (`vehicle_manufacturer_name`) \
                VALUES \
                    ('{vehicle_manufacturer}') \
                ;"
        self.db_cursor.execute(query)
