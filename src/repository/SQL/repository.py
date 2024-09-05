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
    def evolução_por_canal(cls, _escopo_trad: int, _escopo_dig: int, data_inicio: str = None, data_fim: str = None):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.conn.cursor()

        try:
            # Convertendo as datas de entrada para o formato adequado, se fornecidas
            if data_inicio:
                data_inicio = datetime.strptime(data_inicio, '%Y-%m').date().replace(day=1)
            if data_fim:
                data_fim = datetime.strptime(data_fim, '%Y-%m').date().replace(day=1)

            # Consulta SQL base sem as condições de data
            query = '''WITH Manifestações_Digitais AS (
                            SELECT Mes_ano, COUNT(*) AS Manifestações_Digitais
                            FROM CAMIL.dbo.Escopo_{0}
                            GROUP BY Mes_ano
                      ), 
                      Manifestações_tradicionais AS (
                            SELECT Mes_ano, COUNT(*) AS Manifestações_tradicionais
                            FROM CAMIL.dbo.Escopo_{1}
                            GROUP BY Mes_ano
                      ) 
                      SELECT COALESCE(c8006.Mes_ano, c9006.Mes_ano) AS Mes_ano, 
                             COALESCE(Manifestações_tradicionais, 0) AS Quantidade_Linhas_tradicional, 
                             COALESCE(Manifestações_Digitais, 0) AS Quantidade_Linhas_digital
                      FROM Manifestações_Digitais c8006 
                      FULL JOIN Manifestações_tradicionais c9006 
                      ON c8006.Mes_ano = c9006.Mes_ano'''.format(_escopo_dig, _escopo_trad)

            params = []

            # Adicionar as condições de data à consulta se fornecidas
            if data_inicio and data_fim:
                query += " WHERE COALESCE(c8006.Mes_ano, c9006.Mes_ano) BETWEEN ? AND ?"
                params.extend([data_inicio, data_fim])

            query += " ORDER BY Mes_ano"

            # Executar a consulta SQL com os parâmetros fornecidos, se houver
            if params:
                database_cursor.execute(query, params)
            else:
                database_cursor.execute(query)

            result_tradicional = database_cursor.fetchall()

            infra_sql.close_connection()

            df_data = []
            for row in result_tradicional:
                mes_ano, tradicional, digital = row
                df_data.append({'Mes_ano': mes_ano, 'Quantidade_Linhas_tradicional': tradicional,
                                'Quantidade_Linhas_digital': digital})

            df_tradicional = pd.DataFrame(df_data)

            return df_tradicional

        except ValueError as e:
            print(f"Erro ao converter datas: {e}")
            return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

        finally:
            infra_sql.close_connection()
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
            SELECT Tipo_de_Ocorrencia COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, 
                   COUNT(*) AS Quantidade 
            INTO #TotalOcorrencias
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) BETWEEN '{data_inicio_1}' AND '{data_fim_1}'
            GROUP BY Tipo_de_Ocorrencia;

            DELETE FROM #TotalOcorrencias 
            WHERE Tipo_de_Ocorrencia = 'Redes Sociais';

            -- Contar ocorrências específicas de Comentários e Menções dentro de Redes Sociais
            SELECT 'COMENTÁRIOS E MENÇÕES' COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, 
                   COUNT(*) AS Quantidade 
            INTO #ComentariosMencoes
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) BETWEEN '{data_inicio_1}' AND '{data_fim_1}'
            AND Tipo_de_Ocorrencia = 'Redes Sociais'
            AND Grupo_de_tipo_de_ocorrencia IN ('Comentários e Menções')
            GROUP BY Tipo_de_Ocorrencia;

            -- Contar Redes Sociais excluindo Comentários e Menções
            SELECT 'OUTRAS INTERAÇÕES' COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, 
                   COUNT(*) AS Quantidade 
            INTO #OutrasInteracoes
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) BETWEEN '{data_inicio_1}' AND '{data_fim_1}'
            AND Tipo_de_Ocorrencia = 'Redes Sociais'
            AND Grupo_de_tipo_de_ocorrencia NOT IN ('Comentários e Menções')
            GROUP BY Tipo_de_Ocorrencia;
        '''

        query_1 = f'''
            SELECT Tipo_de_Ocorrencia, Quantidade 
            FROM #TotalOcorrencias
            UNION ALL
            SELECT Tipo_de_Ocorrencia COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, Quantidade 
            FROM #ComentariosMencoes
            UNION ALL
            SELECT Tipo_de_Ocorrencia COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, Quantidade 
            FROM #OutrasInteracoes;

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
    def repren_tipos_ocorrencia_final_period(cls, _escopo: int, data_fim: str):
        infra_sql = InfrastructureSQL()
        infra_sql.connect()
        database_cursor = infra_sql.conn.cursor()

        # Converter a data de fim para o formato apropriado
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        # Ajustar a query para garantir o collation
        query = f'''
            -- Criar tabela temporária para total de ocorrências
            SELECT Tipo_de_Ocorrencia COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, 
                   COUNT(*) AS Quantidade 
            INTO #TotalOcorrencias
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) = '{data_fim}'
            GROUP BY Tipo_de_Ocorrencia;

            DELETE FROM #TotalOcorrencias 
            WHERE Tipo_de_Ocorrencia = 'Redes Sociais';

            -- Contar ocorrências específicas de Comentários e Menções dentro de Redes Sociais
            SELECT 'COMENTÁRIOS E MENÇÕES' COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, 
                   COUNT(*) AS Quantidade 
            INTO #ComentariosMencoes
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) = '{data_fim}'
            AND Tipo_de_Ocorrencia = 'Redes Sociais'
            AND Grupo_de_tipo_de_ocorrencia IN ('Comentários e Menções')
            GROUP BY Tipo_de_Ocorrencia;

            -- Contar Redes Sociais excluindo Comentários e Menções
            SELECT 'OUTRAS INTERAÇÕES' COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, 
                   COUNT(*) AS Quantidade 
            INTO #OutrasInteracoes
            FROM Escopo_{_escopo} 
            WHERE CONVERT(date, Mes_ano) = '{data_fim}'
            AND Tipo_de_Ocorrencia = 'Redes Sociais'
            AND Grupo_de_tipo_de_ocorrencia NOT IN ('Comentários e Menções')
            GROUP BY Tipo_de_Ocorrencia;
        '''

        query_1 = f'''
            -- Unir resultados das tabelas temporárias
            SELECT Tipo_de_Ocorrencia, Quantidade 
            FROM #TotalOcorrencias
            UNION ALL
            SELECT Tipo_de_Ocorrencia COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, Quantidade 
            FROM #ComentariosMencoes
            UNION ALL
            SELECT Tipo_de_Ocorrencia COLLATE Latin1_General_CI_AS AS Tipo_de_Ocorrencia, Quantidade 
            FROM #OutrasInteracoes;

            -- Limpar tabelas temporárias
            DROP TABLE #TotalOcorrencias;
            DROP TABLE #ComentariosMencoes;
            DROP TABLE #OutrasInteracoes;
        '''

        # Executar a consulta e obter os resultados
        database_cursor.execute(query)
        database_cursor.execute(query_1)
        results = database_cursor.fetchall()

        # Converter os resultados em DataFrame
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

        # Formatação segura para o nome da tabela
        escopo_table = f"Escopo_{escopo}"

        query = f'''
        WITH CTE AS (
            SELECT 
                CASE 
                    WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                    WHEN grupo_de_tipo_de_ocorrencia = 'SABOR/ODOR ALTERADO' THEN 'Sabor/Odor Alterado'
                    ELSE ocorrencia
                END AS Ocorrencia,
                COUNT(*) AS Quantidade
            FROM {escopo_table}
            WHERE Tipo_de_ocorrencia = 'Reclamação'
              AND CONVERT(date, Mes_ano) BETWEEN ? AND ? 
            GROUP BY 
                CASE 
                    WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                    WHEN grupo_de_tipo_de_ocorrencia = 'SABOR/ODOR ALTERADO' THEN 'Sabor/Odor Alterado'
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
        '''

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

        # Converte data_fim para o formato correto
        data_fim = datetime.strptime(data_fim, '%Y-%m').date()
        data_fim = data_fim.replace(day=1)

        # Formatação segura para o nome da tabela
        escopo_table = f"Escopo_{escopo}"

        query = f'''
              WITH CTE AS (
                  SELECT 
                      CASE 
                          WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                          WHEN grupo_de_tipo_de_ocorrencia = 'SABOR/ODOR ALTERADO' THEN 'Sabor/Odor Alterado'
                          ELSE ocorrencia
                      END AS Ocorrencia,
                      COUNT(*) AS Quantidade
                  FROM {escopo_table}
                  WHERE Tipo_de_ocorrencia = 'Reclamação'
                    AND CONVERT(date, Mes_ano) = ?
                  GROUP BY 
                      CASE 
                          WHEN grupo_de_tipo_de_ocorrencia = 'INFESTAÇÃO' THEN 'Infestação'
                          WHEN grupo_de_tipo_de_ocorrencia = 'SABOR/ODOR ALTERADO' THEN 'Sabor/Odor Alterado'
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
              '''

        # Executa a consulta com o parâmetro data_fim
        database_cursor.execute(query, (data_fim,))
        results = database_cursor.fetchall()

        # Converte os resultados para um DataFrame
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
