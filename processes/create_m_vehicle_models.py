import db_connection as db


class create_m_vehicle_models:

    def __init__(self):
        try:
            self.db_cnt = db.db_connection().get_connection()
            self.db_cursor = self.db_cnt.cursor()
        except:
            print('create_m_vehicle_models init except')

    def execute(self, maching_data_id):
        try:
            self.db_cnt.autocommit(False)

            repair_data = self.__get_repair_data(maching_data_id)
            for item in repair_data:
                if (item['vehicle_model'] is None):
                    continue

                vehicle_model = item['vehicle_model'].strip()
                vehicle_manufacturer = item['vehicle_manufacturer'].strip()

                if (self.__is_vehicle_model_name_already_exist(vehicle_model)):
                    m_vehicle_manufacturer_id = self.__get_m_vehicle_manufacturer_id(
                        vehicle_manufacturer)
                    if (m_vehicle_manufacturer_id is False):
                        raise Exception(
                            '[create_m_vehicle_models - ERROR] [update] m_vehicle_manufacturer_id were not find.')
                    self.__update_m_vehicle_models(
                        m_vehicle_manufacturer_id, vehicle_model)
                else:
                    m_vehicle_manufacturer_id = self.__get_m_vehicle_manufacturer_id(
                        vehicle_manufacturer)
                    if (m_vehicle_manufacturer_id is False):
                        raise Exception(
                            '[create_m_vehicle_models - ERROR] [create] m_vehicle_manufacturer_id were not find.')
                    
                    self.__create_m_vehicle_models(
                        m_vehicle_manufacturer_id, vehicle_model)

            self.db_cnt.commit()
            return True
        except Exception as e:
            self.db_cnt.rollback()
            print(e)
            return False
        finally:
            self.db_cnt.close()

    def __get_repair_data(self, maching_data_id):
        query = f"SELECT \
                    `vehicle_model`, \
                    `vehicle_manufacturer` \
                FROM \
	                `t_repair_data` \
                WHERE \
                    `t_matching_data_id` = {maching_data_id} \
                    AND `deleted_at` IS NULL \
                GROUP BY \
                    `vehicle_model`, \
                    `vehicle_manufacturer` \
                ;"
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def __is_vehicle_model_name_already_exist(self, vehicle_model):
        query = f"SELECT \
                    `vehicle_model_name` \
                FROM \
	                `m_vehicle_models` \
                WHERE \
                    `vehicle_model_name` = '{vehicle_model}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        if (self.db_cursor.fetchone() is None):
            return False
        return True

    def __get_m_vehicle_manufacturer_id(self, vehicle_manufacturer):
        query = f"SELECT \
                    `id` \
                FROM \
	                `m_vehicle_manufacturers` \
                WHERE \
                    `vehicle_manufacturer_name` = '{vehicle_manufacturer}' \
                    AND `deleted_at` IS NULL \
                LIMIT 1 \
                ;"
        self.db_cursor.execute(query)
        data = self.db_cursor.fetchone()
        if (data is None):
            return False
        return data['id']

    def __update_m_vehicle_models(self, m_vehicle_manufacturer_id, vehicle_model):
        query = f"UPDATE \
	                `m_vehicle_models` \
                SET \
                    `m_vehicle_manufacturer_id` = {m_vehicle_manufacturer_id} \
                WHERE \
                    `vehicle_model_name` = '{vehicle_model}' \
                ;"
        self.db_cursor.execute(query)

    def __create_m_vehicle_models(self, m_vehicle_manufacturer_id, vehicle_model):
        query = f"INSERT INTO \
	                `m_vehicle_models` (`m_vehicle_manufacturer_id`, `vehicle_model_name`) \
                VALUES \
                    ({m_vehicle_manufacturer_id}, '{vehicle_model}') \
                ;"
        self.db_cursor.execute(query)
