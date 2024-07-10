from decimal import Decimal
from src.infrastructure.SQLServer.infrastructure import InfrastructureSQL
import pandas as pd
from datetime import datetime

class SQLRepository:

    @classmethod
    def get_escopo_name_bd(cls, escopo: int):
        query = 'SELECT Nome_Escopo FROM CAMIL_ESCOPO WHERE Escopo = ?'
        params = (escopo,)
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
        data_inicio_1 = data_inicio.replace(day=1)
        data_fim_1 = data_fim.replace(day=1)

        query = f'''
            SELECT Tipo_de_Ocorrencia, COUNT(*) AS Quantidade 
            INTO #TotalOcorrencias
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) BETWEEN '{data_inicio_1}' AND '{data_fim_1}'
            GROUP BY Tipo_de_Ocorrencia;

            DELETE FROM #TotalOcorrencias WHERE Tipo_de_Ocorrencia = 'Redes Sociais';

            -- Contar ocorrências específicas de Comentários e Menções dentro de Redes Sociais
            SELECT 'COMENTÁRIOS E MENÇÕES' AS Tipo_de_Ocorrencia, COUNT(*) AS Quantidade 
            INTO #ComentariosMencoes
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) BETWEEN '{data_inicio_1}' AND '{data_fim_1}'
            AND Tipo_de_Ocorrencia = 'Redes Sociais'
            AND Grupo_de_tipo_de_ocorrencia IN ('Comentários e Menções')
            GROUP BY Tipo_de_Ocorrencia;

            -- Contar Redes Sociais excluindo Comentários e Menções
            SELECT 'OUTRAS INTERAÇÕES' AS Tipo_de_Ocorrencia, COUNT(*) AS Quantidade 
            INTO #OutrasInteracoes
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) BETWEEN '{data_inicio_1}' AND '{data_fim_1}'
            AND Tipo_de_Ocorrencia = 'Redes Sociais'
            AND Grupo_de_tipo_de_ocorrencia NOT IN ('Comentários e Menções')
            GROUP BY Tipo_de_Ocorrencia;'''

        query_1 = f'''
            SELECT * FROM #TotalOcorrencias
            UNION ALL
            SELECT * FROM #ComentariosMencoes
            UNION ALL
            SELECT * FROM #OutrasInteracoes;

            -- Limpar tabelas temporárias
            DROP TABLE #TotalOcorrencias;
            DROP TABLE #ComentariosMencoes;
            DROP TABLE #OutrasInteracoes;
        '''

        database_cursor.execute(query)
        database_cursor.execute(query_1)
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
    def repren_tipos_ocorrencia_final_period(cls, _escopo: int, data_fim: str):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.conn.cursor()

        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        query = f'''
                    SELECT Tipo_de_Ocorrencia, COUNT(*) AS Quantidade 
                    INTO #TotalOcorrencias
                    FROM Escopo_{_escopo} 
                    WHERE CONVERT(date, Mes_ano) ='{data_fim}'
                    GROUP BY Tipo_de_Ocorrencia;

                    DELETE FROM #TotalOcorrencias WHERE Tipo_de_Ocorrencia = 'Redes Sociais';

                    -- Contar ocorrências específicas de Comentários e Menções dentro de Redes Sociais
                    SELECT 'COMENTÁRIOS E MENÇÕES' AS Tipo_de_Ocorrencia, COUNT(*) AS Quantidade 
                    INTO #ComentariosMencoes
                    FROM Escopo_{_escopo} 
                    WHERE CONVERT(date, Mes_ano) = '{data_fim}'
                    AND Tipo_de_Ocorrencia = 'Redes Sociais'
                    AND Grupo_de_tipo_de_ocorrencia IN ('Comentários e Menções')
                    GROUP BY Tipo_de_Ocorrencia;

                    -- Contar Redes Sociais excluindo Comentários e Menções
                    SELECT 'OUTRAS INTERAÇÕES' AS Tipo_de_Ocorrencia, COUNT(*) AS Quantidade 
                    INTO #OutrasInteracoes
                    FROM Escopo_{_escopo} 
                    WHERE CONVERT(date, Mes_ano) = '{data_fim}'
                    AND Tipo_de_Ocorrencia = 'Redes Sociais'
                    AND Grupo_de_tipo_de_ocorrencia NOT IN ('Comentários e Menções')
                    GROUP BY Tipo_de_Ocorrencia;'''

        query_1 = f'''
                    SELECT * FROM #TotalOcorrencias
                    UNION ALL
                    SELECT * FROM #ComentariosMencoes
                    UNION ALL
                    SELECT * FROM #OutrasInteracoes;

                    -- Limpar tabelas temporárias
                    DROP TABLE #TotalOcorrencias;
                    DROP TABLE #ComentariosMencoes;
                    DROP TABLE #OutrasInteracoes;
                '''

        database_cursor.execute(query)
        database_cursor.execute(query_1)
        results = database_cursor.fetchall()
        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df

    @classmethod
    def chart_columns_line_reclamacao(cls, escopo, data_inicio, data_fim):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.cursor_db()
        data_inicio = datetime.strptime(data_inicio, '%Y-%m').date()
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_inicio = data_inicio.replace(day=1)
        data_fim = data_fim.replace(day=1)

        query = '''WITH CTE AS (
            SELECT 
                CASE 
                    WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                    ELSE ocorrencia
                END AS Ocorrencia,
                COUNT(*) AS Quantidade
            FROM CAMIL.dbo.Escopo_{}
            WHERE Tipo_de_ocorrencia = 'Reclamação'
              AND CONVERT(date, Mes_ano) BETWEEN ? AND ? 
            GROUP BY 
                CASE 
                    WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                    ELSE ocorrencia
                END
        ),
        Top10 AS (
            SELECT 
                Ocorrencia,
                Quantidade,
                0 AS Sort_order
            FROM CTE
            ORDER BY Quantidade DESC
            OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
        ),
        Remaining AS (
            SELECT 
                'Reclamações em menor volume' AS Ocorrencia,
                SUM(Quantidade) AS Quantidade,
                1 AS Sort_order
            FROM CTE
            WHERE Ocorrencia NOT IN (SELECT Ocorrencia FROM Top10)
        ),
        FinalResult AS (
            SELECT Ocorrencia, Quantidade, Sort_order FROM Top10
            UNION ALL
            SELECT Ocorrencia, Quantidade, Sort_order FROM Remaining
        )
        SELECT Ocorrencia, Quantidade
        FROM FinalResult
        ORDER BY Sort_order, Quantidade DESC;
        '''.format(escopo)

        database_cursor.execute(query, (data_inicio, data_fim))
        results = database_cursor.fetchall()

        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df

    @classmethod
    def chart_columns_line_reclamacao_last_date(cls, escopo, data_fim):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.cursor_db()
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        query = '''WITH CTE AS (
                SELECT 
                    CASE 
                        WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                        ELSE ocorrencia
                    END AS Ocorrencia,
                    COUNT(*) AS Quantidade
                FROM CAMIL.dbo.Escopo_{}
                WHERE Tipo_de_ocorrencia = 'Reclamação'
                  AND CONVERT(date, Mes_ano) = ?
                GROUP BY 
                    CASE 
                        WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                        ELSE ocorrencia
                    END
            ),
            Top10 AS (
                SELECT 
                    Ocorrencia,
                    Quantidade,
                    0 AS Sort_order
                FROM CTE
                ORDER BY Quantidade DESC
                OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
            ),
            Remaining AS (
                SELECT 
                    'Reclamações em menor volume' AS Ocorrencia,
                    SUM(Quantidade) AS Quantidade,
                    1 AS Sort_order
                FROM CTE
                WHERE Ocorrencia NOT IN (SELECT Ocorrencia FROM Top10)
            ),
            FinalResult AS (
                SELECT Ocorrencia, Quantidade, Sort_order FROM Top10
                UNION ALL
                SELECT Ocorrencia, Quantidade, Sort_order FROM Remaining
            )
            SELECT Ocorrencia, Quantidade
            FROM FinalResult
            ORDER BY Sort_order, Quantidade DESC;
            '''.format(escopo)

        database_cursor.execute(query, (data_fim))
        results = database_cursor.fetchall()

        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df
    @classmethod
    def chart_columns_line_except_reclamacao(cls, escopo, data_inicio, data_fim):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.cursor_db()
        data_inicio = datetime.strptime(data_inicio, '%Y-%m').date()
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_inicio = data_inicio.replace(day=1)
        data_fim = data_fim.replace(day=1)

        query = '''WITH CTE AS (
    SELECT 
        LOWER(Tipo_de_ocorrencia + ' - ' + ocorrencia) AS ocorrencia,
        COUNT(*) AS quantidade
    FROM Escopo_{}
    WHERE Tipo_de_ocorrencia != 'RECLAMAÇÃO'
      AND CONVERT(date, Mes_ano) BETWEEN ? AND ? 
    GROUP BY 
        Tipo_de_ocorrencia + ' - ' + ocorrencia
),
Top10 AS (
    SELECT 
        ocorrencia,
        quantidade,
        0 AS sort_order
    FROM CTE
    ORDER BY quantidade DESC
    OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
),
Remaining AS (
    SELECT 
        'Manifestações em menor volume' AS ocorrencia,
        SUM(quantidade) AS quantidade,
        1 AS sort_order
    FROM CTE
    WHERE ocorrencia NOT IN (SELECT ocorrencia FROM Top10)
),
FinalResult AS (
    SELECT ocorrencia, quantidade, sort_order FROM Top10
    UNION ALL
    SELECT ocorrencia, quantidade, sort_order FROM Remaining
)
SELECT ocorrencia, quantidade
FROM FinalResult
ORDER BY sort_order, quantidade DESC;

        '''.format(escopo)

        database_cursor.execute(query, (data_inicio, data_fim))
        results = database_cursor.fetchall()

        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df

    @classmethod
    def chart_columns_line_except_reclamacao_last_date(cls, escopo, data_fim):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.cursor_db()
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        query = '''WITH CTE AS (
    SELECT 
        LOWER(Tipo_de_ocorrencia + ' - ' + ocorrencia) AS ocorrencia,
        COUNT(*) AS quantidade
    FROM Escopo_{}
    WHERE Tipo_de_ocorrencia != 'RECLAMAÇÃO'
      AND CONVERT(date, Mes_ano) = ?
    GROUP BY 
        Tipo_de_ocorrencia + ' - ' + ocorrencia
),
Top10 AS (
    SELECT 
        ocorrencia,
        quantidade,
        0 AS sort_order
    FROM CTE
    ORDER BY quantidade DESC
    OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
),
Remaining AS (
    SELECT 
        'Manifestações em menor volume' AS ocorrencia,
        SUM(quantidade) AS quantidade,
        1 AS sort_order
    FROM CTE
    WHERE ocorrencia NOT IN (SELECT ocorrencia FROM Top10)
),
FinalResult AS (
    SELECT ocorrencia, quantidade, sort_order FROM Top10
    UNION ALL
    SELECT ocorrencia, quantidade, sort_order FROM Remaining
)
SELECT ocorrencia, quantidade
FROM FinalResult
ORDER BY sort_order, quantidade DESC;

        '''.format(escopo)

        database_cursor.execute(query, (data_fim))
        results = database_cursor.fetchall()

        df_data = [(row[0], row[1]) for row in results]
        df = pd.DataFrame(df_data, columns=['Tipo_de_ocorrencia', 'Total'])

        infra_sql.close_connection()

        return df

    @classmethod
    def total_per_category(cls, escopo, data_inicio, data_fim):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.cursor_db()
        data_inicio = datetime.strptime(data_inicio, '%Y-%m').date()
        data_inicio = data_inicio.replace(day=1)
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        query = f"""
                           DECLARE @cols AS NVARCHAR(MAX),
                           @query AS NVARCHAR(MAX);

                           -- Obter a lista de marcas distintas, substituindo "EM GERAL" por "Não identificada"
                           SELECT @cols = STRING_AGG(QUOTENAME(CASE WHEN Marca_MAENA = 'EM GERAL' THEN 'Não identificada' ELSE Marca_MAENA END), ', ')
                           WITHIN GROUP (ORDER BY CASE WHEN Marca_MAENA = 'EM GERAL' THEN 1 ELSE 0 END)
                           FROM (SELECT DISTINCT Marca_MAENA
                                 FROM Escopo_{escopo}
                                 WHERE Marca_MAENA IS NOT NULL) AS tmp;

                           -- Construir a consulta dinâmica PIVOT
                           SET @query = '
                           SELECT SubCategoria, ' + @cols + ' 
                           FROM (
                               SELECT 
                                   COALESCE(SubCategoria_nivel1, ''geral'') AS SubCategoria,
                                   CASE WHEN Marca_MAENA = ''EM GERAL'' THEN ''Não identificada'' ELSE Marca_MAENA END AS Marca,
                                   COUNT(*) AS Total_Manifestacoes
                               FROM 
                                   Escopo_{escopo}
                               WHERE 
                                   CONVERT(date, Mes_ano) BETWEEN ''{data_inicio}'' AND ''{data_fim}''
                               GROUP BY 
                                   COALESCE(SubCategoria_nivel1, ''geral''),
                                   CASE WHEN Marca_MAENA = ''EM GERAL'' THEN ''Não identificada'' ELSE Marca_MAENA END
                           ) AS SourceTable
                           PIVOT (
                               SUM(Total_Manifestacoes)
                               FOR Marca IN (' + @cols + ')
                           ) AS PivotTable';

                           -- Adicionar a substituição de NULL por 0 na seleção final
                          SET @query = 'SELECT SubCategoria, ' + 
                     (SELECT STRING_AGG('ISNULL(' + QUOTENAME(CASE WHEN Marca_MAENA = 'EM GERAL' THEN 'Não identificada' ELSE Marca_MAENA END) + ', 0) AS ' + QUOTENAME(CASE WHEN Marca_MAENA = 'EM GERAL' THEN 'Não identificada' ELSE Marca_MAENA END), ', ')
                      FROM (SELECT DISTINCT Marca_MAENA
                            FROM Escopo_{escopo}
                            WHERE Marca_MAENA IS NOT NULL) AS tmp)
                     + ' FROM (' + @query + ') AS FinalResult ORDER BY SubCategoria ASC';

                           -- Executar a consulta dinâmica
                           EXEC sp_executesql @query;
                       """

        database_cursor.execute(query)
        results = database_cursor.fetchall()

        if not results:
            return pd.DataFrame()  # Retorna um DataFrame vazio se não houver resultados

        # Obter os nomes das colunas do cursor
        columns = [column[0] for column in database_cursor.description]

        # Converter os resultados para uma lista de tuplas
        data = [tuple(row) for row in results]

        df = pd.DataFrame(data, columns=columns)

        return df

    @classmethod
    def total_per_category_period(cls, escopo, data_fim):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.cursor_db()

        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        query = f"""
                   DECLARE @cols AS NVARCHAR(MAX),
                   @query AS NVARCHAR(MAX);

                   -- Obter a lista de marcas distintas, substituindo "EM GERAL" por "Não identificada"
                   SELECT @cols = STRING_AGG(QUOTENAME(CASE WHEN Marca_MAENA = 'EM GERAL' THEN 'Não identificada' ELSE Marca_MAENA END), ', ')
                   WITHIN GROUP (ORDER BY CASE WHEN Marca_MAENA = 'EM GERAL' THEN 1 ELSE 0 END)
                   FROM (SELECT DISTINCT Marca_MAENA
                         FROM Escopo_{escopo}
                         WHERE Marca_MAENA IS NOT NULL) AS tmp;

                   -- Construir a consulta dinâmica PIVOT
                   SET @query = '
                   SELECT SubCategoria, ' + @cols + ' 
                   FROM (
                       SELECT 
                           COALESCE(SubCategoria_nivel1, ''geral'') AS SubCategoria,
                           CASE WHEN Marca_MAENA = ''EM GERAL'' THEN ''Não identificada'' ELSE Marca_MAENA END AS Marca,
                           COUNT(*) AS Total_Manifestacoes
                       FROM 
                           Escopo_{escopo}
                       WHERE 
                           CONVERT(date, Mes_ano) = ''{data_fim}''
                       GROUP BY 
                           COALESCE(SubCategoria_nivel1, ''geral''),
                           CASE WHEN Marca_MAENA = ''EM GERAL'' THEN ''Não identificada'' ELSE Marca_MAENA END
                   ) AS SourceTable
                   PIVOT (
                       SUM(Total_Manifestacoes)
                       FOR Marca IN (' + @cols + ')
                   ) AS PivotTable';

                   -- Adicionar a substituição de NULL por 0 na seleção final
                  SET @query = 'SELECT SubCategoria, ' + 
             (SELECT STRING_AGG('ISNULL(' + QUOTENAME(CASE WHEN Marca_MAENA = 'EM GERAL' THEN 'Não identificada' ELSE Marca_MAENA END) + ', 0) AS ' + QUOTENAME(CASE WHEN Marca_MAENA = 'EM GERAL' THEN 'Não identificada' ELSE Marca_MAENA END), ', ')
              FROM (SELECT DISTINCT Marca_MAENA
                    FROM Escopo_{escopo}
                    WHERE Marca_MAENA IS NOT NULL) AS tmp)
             + ' FROM (' + @query + ') AS FinalResult ORDER BY SubCategoria ASC';

                   -- Executar a consulta dinâmica
                   EXEC sp_executesql @query;
               """

        database_cursor.execute(query)
        results = database_cursor.fetchall()

        if not results:
            return pd.DataFrame()  # Retorna um DataFrame vazio se não houver resultados

        # Obter os nomes das colunas do cursor
        columns = [column[0] for column in database_cursor.description]

        # Converter os resultados para uma lista de tuplas
        data = [tuple(row) for row in results]

        df = pd.DataFrame(data, columns=columns)

        return df
