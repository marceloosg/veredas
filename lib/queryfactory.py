import mysql.connector
import mysql
import pandas as pd
import time


class QueryFactory:
    def connect(self):
        if not (self.conn and self.conn.is_connected()):
            self.conn = mysql.connector.connect(
                host=self.credentials["host"],
                user=self.credentials["user"],
                password=self.credentials["pass"],
                db=self.credentials["db"],
                charset='utf8',
                buffered=True,
                connection_timeout=180,
                autocommit=True)
            cur = self.conn.cursor()
            cur.execute('set session net_read_timeout=120')
            cur.execute('SET SESSION CHARACTER_SET_RESULTS = latin1')
            cur.close()

    def query(self, query):
        self.connect()
        output=None
        ea=None
        for i in range(3):
            try:
                output=pd.read_sql(query, con=self.conn)
            except Exception as e:
                output=None
                time.sleep(1)
                ea=e
        if output is not None:
            return output
        else:
            raise ea

    def __init__(self, credentials):
        self.conn = None
        self.credentials = credentials

