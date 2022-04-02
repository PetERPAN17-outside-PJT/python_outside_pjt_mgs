import db_connection as db


class update_m_billing_destinations:

    def __init__(self):
        try:
            self.db_cnt = db.db_connection().get_connection()
            self.db_cursor = self.db_cnt.cursor()
        except:
            print('update_billing_destinations init except')

    def execute(self, maching_data_id):
        try:
            self.db_cnt.autocommit(False)
            repair_data = self.__get_repair_data(maching_data_id)
            for item in repair_data:
                update_value = self.__get_update_value(
                    item['billing_destination_category_name'].strip())

                self.__update_billing_destination_category(
                    update_value, item['billing_destination_code'].strip())

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
	                `billing_destination_code`, \
                    `billing_destination_category_name` \
                FROM \
	                `t_repair_data` \
                WHERE \
                    `t_matching_data_id` = {maching_data_id} \
                    AND `deleted_at` IS NULL \
                GROUP BY \
                    `billing_destination_code`, \
                    `billing_destination_category_name` \
                ;"
        self.db_cursor.execute(query)
        return self.db_cursor.fetchall()

    def __get_update_value(self, billing_destination_category_name):
        if (billing_destination_category_name == '1 MGS' or billing_destination_category_name == '2 ｸﾞﾙｰﾌﾟ'):
            return 1
        return 2

    def __update_billing_destination_category(self, update_value, billing_destination_code):
        query = f"UPDATE \
	                `m_billing_destinations` \
                SET \
                    `billing_destination_category` = {update_value} \
                WHERE \
                    `billing_destination_code` = {billing_destination_code} \
                ;"
        self.db_cursor.execute(query)
