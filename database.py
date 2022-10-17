import sqlite3 as sql

class DataBase:

    def __init__(self, filename):
        self.connection = sql.connect(filename, check_same_thread=False)
        pass

    def __del__(self):
        self.connection.commin()
        self.connection.close()

    
