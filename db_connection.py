import sys
import os
import pymysql.cursors


class db_connection:

    def __init__(self):
        try:
            self.connection = pymysql.connect(
                host='',
                user='',
                password='',
                db='',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Success connecting to RDS mysql instance")
        except Exception as e:
            print(e)
            print("Fail connecting to RDS mysql instance")
            sys.exit()

    def get_connection(self):
        return self.connection
