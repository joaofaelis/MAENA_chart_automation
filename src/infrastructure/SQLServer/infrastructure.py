import pyodbc
from decouple import config

class InfrastructureSQL:

    def __init__(self):
        self.server = config('SERVER')
        self.database = config('DATABASE')
        self.conn = None

    def connect(self, trusted_connection=True):
        try:
            conn_str = (
                f'DRIVER=ODBC Driver 17 for SQL Server;'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'TRUSTED_CONNECTION={"yes" if trusted_connection else "no"};'
            )
            self.conn = pyodbc.connect(conn_str)
            print("Conexão estabelecida com sucesso.")
        except Exception as e:
            print(f"Erro ao conectar ao SQL Server: {e}")
            return self.conn

    def close_connection(self):
        try:
            self.conn.close()
            print("Conexão fechada com sucesso.")
        except Exception as e:
            print(f"Erro ao fechar conexão: {e}")

    def cursor_db(self):
        self.connect()
        return self.conn.cursor()