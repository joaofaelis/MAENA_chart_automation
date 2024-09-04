import pyodbc
from decouple import config

class InfrastructureSQL:

    def __init__(self):
        self.server = config('SERVER')
        self.database = config('DATABASE')
        self.username = config('USERNAME')
        self.password = config('PASSWORD')
        self.conn = None

    def connect(self):
        try:
            conn_str = (
                f'DRIVER={{ODBC Driver 18 for SQL Server}};'
                f'SERVER=tcp:{self.server},1433;'
                f'DATABASE={self.database};'
                f'UID=maena-ia;'
                f'PWD={self.password};'
                f'Encrypt=yes;'
                f'TrustServerCertificate=no;'
                f'Connection Timeout=30;'
            )
            self.conn = pyodbc.connect(conn_str)
        except Exception as e:
            print(f"Erro ao conectar ao SQL Server: {e}")
            self.conn = None

    def close_connection(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            print(f"Erro ao fechar conexão: {e}")

    def cursor_db(self):
        self.connect()
        if self.conn:
            return self.conn.cursor()
        else:
            return None


if __name__ == "__main__":
    db = InfrastructureSQL()
    cursor = db.cursor_db()

