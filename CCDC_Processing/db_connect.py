import psycopg2


class DBConnect(object):
    """
    Class for connecting to a localhost postgresql database
    """

    def __init__(self, host, port, database, user, password, autocommit=False):
        self.conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        try:
            self.cursor = self.conn.cursor()
        except psycopg2.Error:
            raise

        self.autocommit = autocommit
        self.fetcharr = []

    def execute(self, sql_str):
        try:
            self.cursor.execute(sql_str)
        except psycopg2.Error:
            raise

        if self.autocommit:
            self.commit()

    def select(self, sql_str, vals):
        if not isinstance(vals, tuple):
            raise TypeError

        self.cursor.execute(sql_str, vals)

        try:
            self.fetcharr = self.cursor.fetchall()
        except psycopg2.Error:
            raise

    def commit(self):
        try:
            self.conn.commit()
        except psycopg2.Error:
            raise

    def rollback(self):
        self.conn.rollback()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def __len__(self):
        return len(self.fetcharr)

    def __iter__(self):
        return iter(self.fetcharr)

    def __getitem__(self, item):
        if item >= len(self.fetcharr):
            raise IndexError
        return self.fetcharr[item]

    def __del__(self):
        self.cursor.close()
        self.conn.close()

        del self.cursor
        del self.conn