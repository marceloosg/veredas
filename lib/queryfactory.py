import mysql.connector
import mysql
import pandas as pd


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
        return pd.read_sql(query, con=self.conn)

    def __init__(self, credentials):
        self.conn = None
        self.credentials = credentials

