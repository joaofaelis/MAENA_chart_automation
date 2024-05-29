from decimal import Decimal
from src.infrastructure.SQLServer.infrastructure import InfrastructureSQL
import pandas as pd
from datetime import datetime

class SQLRepository:

    @classmethod
    def get_escopo_name_bd(cls, _id: int):
        query = 'SELECT Nome_Escopo_Hierarquia FROM CAMIL_ESCOPO WHERE Escopo = ?'
        params = (_id,)
        infra = InfrastructureSQL()
        infra.connect()



        if infra is None:
            raise ConnectionError("Falha ao conectar ao banco de dados")

        try:
            cursor = infra.conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchall()

            # DataFrame
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)

            return df
        except Exception as e:
            print(f"Erro ao executar a consulta: {e}")
            return None
        finally:
            infra.close_connection()
    @classmethod
    def evolução_por_canal(cls, _escopo_trad: int, _escopo_dig: int, data_inicio: str, data_fim: str):
            infra_sql = InfrastructureSQL()
            infra_sql.connect()
            database_cursor = infra_sql.conn.cursor()

            # Convertendo as datas de entrada para o formato adequado
            data_inicio = datetime.strptime(data_inicio, '%Y-%m').date()
            data_fim = datetime.strptime(data_fim, '%Y-%m').date()

            # Definindo o primeiro dia do mês para as datas de início e fim
            data_inicio = data_inicio.replace(day=1)
            data_fim = data_fim.replace(day=1)

            # Consulta SQL com parâmetros para as datas
            query = '''WITH Manifestações_Digitais AS (
                            SELECT Mes_ano, COUNT(*) AS Manifestações_Digitais
                            FROM CAMIL.dbo.Escopo_{0}
                            WHERE Mes_ano BETWEEN ? AND ?
                            GROUP BY Mes_ano
                      ), 
                      Manifestações_tradicionais AS (
                            SELECT Mes_ano, COUNT(*) AS Manifestações_tradicionais
                            FROM CAMIL.dbo.Escopo_{1}
                            WHERE Mes_ano BETWEEN ? AND ?
                            GROUP BY Mes_ano
                      ) 
                      SELECT COALESCE(c8006.Mes_ano, c9006.Mes_ano) AS Mes_ano, 
                             COALESCE(Manifestações_tradicionais, 0) AS Quantidade_Linhas_tradicional, 
                             COALESCE(Manifestações_Digitais, 0) AS Quantidade_Linhas_digital
                      FROM Manifestações_Digitais c8006 
                      FULL JOIN Manifestações_tradicionais c9006 
                      ON c8006.Mes_ano = c9006.Mes_ano 
                      ORDER BY Mes_ano'''.format(_escopo_dig, _escopo_trad)

            database_cursor.execute(query, (data_inicio, data_fim, data_inicio, data_fim))
            result_tradicional = database_cursor.fetchall()

            infra_sql.close_connection()

            df_data = []
            for row in result_tradicional:
                mes_ano, tradicional, digital = row
                df_data.append({'Mes_ano': mes_ano, 'Quantidade_Linhas_tradicional': tradicional,
                                'Quantidade_Linhas_digital': digital})

            df_tradicional = pd.DataFrame(df_data)

            return df_tradicional
    @classmethod
    def repren_tipos_ocorrencia_timestamp_period(cls, _escopo: int, data_inicio: str, data_fim: str):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.conn.cursor()

        data_inicio = datetime.strptime(data_inicio, '%Y-%m').date()
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_inicio = data_inicio.replace(day=1)
        data_fim = data_fim.replace(day=1)

        query = '''
      SELECT 
    CASE 
        WHEN ID = 290 THEN 'OUTRAS INTERAÇÕES' 
        ELSE Tipo_de_ocorrencia
    END AS Tipo_de_ocorrencia, 
    SUM(Total) AS Total 
FROM CAMIL_MANIFESTACAO_CUBO 
WHERE Escopo = ? AND ID != 1
   AND CONVERT(date, Mes) >= ?
    AND CONVERT(date, Mes) <= ?
    AND (
        (ID = 290 AND Grupo_de_tipo_de_ocorrencia != 'COMENTÁRIOS E MENÇÕES') 
        OR 
        (ID != 290 AND Tipo_de_ocorrencia != 'REDES SOCIAIS')
    )
GROUP BY 
    CASE 
        WHEN ID = 290 THEN 'OUTRAS INTERAÇÕES' 
        ELSE Tipo_de_ocorrencia 
    END

UNION ALL 

SELECT 
    'COMENTÁRIOS E MENÇÕES' AS Tipo_de_ocorrencia, 
    SUM(Total) AS Total
FROM CAMIL_MANIFESTACAO_CUBO 
WHERE Escopo = ?
    AND ID = 292 
    AND CONVERT(date, Mes) >= ? 
    AND CONVERT(date, Mes) <= ?;

        '''

        database_cursor.execute(query, (_escopo, data_inicio, data_fim, _escopo, data_inicio, data_fim))
        results = database_cursor.fetchall()

        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df
    @classmethod
    def get_total_manifestation_bd(cls, escopo: int, data_inicio: str, data_fim: str):
        query = 'SELECT Escopo, ID, Tipo_de_ocorrencia, Grupo_de_tipo_de_ocorrencia, Ocorrencia, Mes, Total FROM CAMIL_MANIFESTACAO_CUBO WHERE ID = 1 AND Escopo = ? AND CONVERT(date, Mes) >= ? AND CONVERT(date, Mes) <= ?'
        params = (escopo, data_inicio, data_fim)
        infra = InfrastructureSQL()
        infra.connect()
        cursor = infra.conn.cursor()
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchall()
        df_data = []
        for row in result:
            converted_row = [float(val) if isinstance(val, Decimal) else val for val in row]
            df_data.append(converted_row)
        df = pd.DataFrame(df_data, columns=columns)

        return df
    @classmethod
    def repren_tipos_ocorrencia_first_period(cls, _escopo: int, data_inicio: str):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.conn.cursor()

        data_inicio = datetime.strptime(data_inicio, '%Y-%m').date()
        data_inicio = data_inicio.replace(day=1)

        query = '''
           SELECT 
    CASE 
        WHEN ID = 290 THEN 'OUTRAS INTERAÇÕES' 
        ELSE Tipo_de_ocorrencia
    END AS Tipo_de_ocorrencia, 
    SUM(Total) AS Total 
FROM CAMIL_MANIFESTACAO_CUBO 
WHERE Escopo = ? AND ID != 1
   AND CONVERT(date, Mes) = ?
    AND (
        (ID = 290 AND Grupo_de_tipo_de_ocorrencia != 'COMENTÁRIOS E MENÇÕES') 
        OR 
        (ID != 290 AND Tipo_de_ocorrencia != 'REDES SOCIAIS')
    )
GROUP BY 
    CASE 
        WHEN ID = 290 THEN 'OUTRAS INTERAÇÕES' 
        ELSE Tipo_de_ocorrencia 
    END

UNION ALL 

SELECT 
    'COMENTÁRIOS E MENÇÕES' AS Tipo_de_ocorrencia, 
    SUM(Total) AS Total
FROM CAMIL_MANIFESTACAO_CUBO 
WHERE Escopo = ?
    AND ID = 292 
    AND CONVERT(date, Mes) = ?;'''

        database_cursor.execute(query, (_escopo, data_inicio, _escopo, data_inicio))
        results = database_cursor.fetchall()

        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df
    @classmethod
    def repren_tipos_ocorrencia_final_period(cls, _escopo: int, data_fim: str):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.conn.cursor()

        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        query = '''
         SELECT 
    CASE 
        WHEN ID = 290 THEN 'OUTRAS INTERAÇÕES' 
        ELSE Tipo_de_ocorrencia
    END AS Tipo_de_ocorrencia, 
    SUM(Total) AS Total 
FROM CAMIL_MANIFESTACAO_CUBO 
WHERE Escopo = ? AND ID != 1
   AND CONVERT(date, Mes) = ?
    AND (
        (ID = 290 AND Grupo_de_tipo_de_ocorrencia != 'COMENTÁRIOS E MENÇÕES') 
        OR 
        (ID != 290 AND Tipo_de_ocorrencia != 'REDES SOCIAIS')
    )
GROUP BY 
    CASE 
        WHEN ID = 290 THEN 'OUTRAS INTERAÇÕES' 
        ELSE Tipo_de_ocorrencia 
    END

UNION ALL 

SELECT 
    'COMENTÁRIOS E MENÇÕES' AS Tipo_de_ocorrencia, 
    SUM(Total) AS Total
FROM CAMIL_MANIFESTACAO_CUBO 
WHERE Escopo = ?
    AND ID = 292 
    AND CONVERT(date, Mes) = ?;

          '''

        database_cursor.execute(query, (_escopo, data_fim, _escopo, data_fim))
        results = database_cursor.fetchall()

        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df


