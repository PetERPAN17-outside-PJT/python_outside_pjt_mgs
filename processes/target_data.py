import db_connection as db


class target_data:

    def __init__(self):
        try:
            self.db_cnt = db.db_connection().get_connection()
            self.db_cursor = self.db_cnt.cursor()
        except:
            print('get_t_repair_data init except.')

    def get_t_matching_data(self):
        try:
            query = f"SELECT \
                        `id` \
                    FROM \
                        `t_matching_data` \
                    WHERE \
                        `is_confirmed` = 1 \
                        AND `is_output_master` = 0 \
                        AND `deleted_at` IS NULL \
                    ORDER BY \
                        `id` ASC \
                    ;"
            self.db_cursor.execute(query)
            return self.db_cursor.fetchall()
        except Exception as e:
            print(f"[target_data - ERROR] in get_t_matching_data. {e}")
        finally:
            self.db_cnt.close()

    def is_t_repair_data_by_m_matching_id_exist(self, matching_data_id):
        try:
            query = f"SELECT \
                        `id` \
                    FROM \
                        `t_repair_data` \
                    WHERE \
                        `t_matching_data_id` = {matching_data_id} \
                        AND `deleted_at` IS NULL \
                    LIMIT 1 \
                    ;"
            self.db_cursor.execute(query)
            if(self.db_cursor.fetchone() is None):
                return False
            return True
        except Exception as e:
            print(
                f"[target_data - ERROR] in is_t_repair_data_by_m_matching_id_exist. {e}")
            return False
        finally:
            self.db_cnt.close()

    def update_is_output_master_to_true(self, matching_data_id):
        try:
            query = f"UPDATE \
                        `t_matching_data` \
                    SET \
                        `is_output_master` = 1 \
                    WHERE \
                        `id` = {matching_data_id} \
                    ;"
            self.db_cursor.execute(query)
            self.db_cnt.commit()
            return True
        except Exception as e:
            print(
                f"[target_data - ERROR] in update_is_output_master_to_true. {e}")
            return e
        finally:
            self.db_cnt.close()
