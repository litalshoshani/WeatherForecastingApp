import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
from AppConfig import MYSQL_HOST, MYSQL_USER, MYSQL_PASS, DB_NAME

class MySqlConnector:
    def __init__(self):
        self.cursor = None
        self.conn = None
        self.connect_to_db()
        #self.create_table_in_db()

    def connect_to_db(self):
        """ Connect to MySQL database """
        try:
            # Connecting to the database. Writes a proper message about connection (succeeded/ failed)
            #self.conn = mysql.connect(host='localhost',database='weather',user='root',password=SECERT_KEY,charset='utf8',use_unicode=True)
            self.conn = mysql.connect(host=MYSQL_HOST,
                                      database=DB_NAME,
                                      user=MYSQL_USER,
                                      password=MYSQL_PASS,
                                      charset='utf8',
                                      use_unicode=True)
            if self.conn.is_connected():
                print("Connected to MySQL database")
                self.cursor = self.conn.cursor()
                return True
        except Error as e:
            print(e)
            return False

    def create_db(self, db_name):
        try:
            if self.conn.is_connected():
                self.cursor.execute("CREATE DATABASE IF NOT EXISTS " + db_name)
        except Error as e:
            print("Failed to created DB. returned error is :")
            print(e)


    def create_table_in_db(self,sql_command):
        if self.conn.is_connected():
            self.cursor.execute("select database();")
            record = self.cursor.fetchone()
            print("You're connected to database: " , record[0])
            print('Creating table...')
            self.cursor.execute(sql_command)
            print('Table is created')

    def insert_row_to_db(self, db_name, table_name, table_row):
        sql_insert = "INSERT INTO " + db_name + "." + table_name + " VALUES (%s,%s,%s,%s,%s)"
        self.cursor.execute(sql_insert, table_row)
        self.conn.commit()

    def execute_query(self, sql_command):
        '''
        The method receives a sql command and execute it.
        :param sql_command:
        :return: 1. the row headers
                2. the result of the query
        '''
        self.cursor.execute(sql_command)
        results = self.cursor.fetchall()
        # get the headers (Temperature and Precipitation)
        row_headers = [x[0] for x in self.cursor.description]
        return results, row_headers