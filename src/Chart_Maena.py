import tkinter as tk
import os
from tkinter import messagebox
from src.repository.SQL.repository import SQLRepository
import xlsxwriter
import pandas as pd
import locale
from datetime import datetime


locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
def run_code():
    escopo = entry_escopo.get()
    escopo_tradicional = entry_escopo_tradicional.get()
    escopo_digital = entry_escopo_digital.get()
    data_inicio = entry_data_inicio.get()
    data_fim = entry_data_fim.get()

    data_inicio_format = datetime.strptime(data_inicio, "%Y-%m")
    data_fim_format = datetime.strptime(data_fim, "%Y-%m")
    data_inicio_formatada = data_inicio_format.strftime("%b/%Y").capitalize()
    data_fim_formatada = data_fim_format.strftime("%b/%Y").capitalize()

    escopo_name = SQLRepository.get_escopo_name_bd(escopo)
    nome_do_escopo = escopo_name['Nome_Escopo'].to_string(index=False)
    titulo_grafico = nome_do_escopo.replace('[', '').replace(']', '')
    titulo_grafico = titulo_grafico.title()

    df_evolucao_p_canal = SQLRepository.evolução_por_canal(escopo_tradicional, escopo_digital, data_inicio, data_fim)
    df_evolucao_p_canal['Mes_ano'] = pd.to_datetime(df_evolucao_p_canal['Mes_ano'], format='%Y-%m')
    df_evolucao_p_canal['Mes_ano'] = df_evolucao_p_canal['Mes_ano'].dt.strftime('%b/%y').str.capitalize()


    df_pie_timestamp = SQLRepository.repren_tipos_ocorrencia_timestamp_period(escopo, data_inicio, data_fim)

    order = {
        'CRÍTICA': 1,
        'ELOGIO': 2,
        'INFORMAÇÃO': 3,
        'RECLAMAÇÃO': 4,
        'SOLICITAÇÃO': 5,
        'SUGESTÃO': 6,
        'COMENTÁRIOS E MENÇÕES': 7,
        'OUTRAS INTERAÇÕES': 8
    }

    points = [
        {'fill': {'color': '#FFD700'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}},
        {'fill': {'color': '#228B22'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}},
        {'fill': {'color': '#6495ED'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}},
        {'fill': {'color': 'red'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}},
        {'fill': {'color': 'F4A460'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}},
        {'fill': {'color': '#FF00FF'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}},
        {'fill': {'color': '#008080'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}},
        {'fill': {'color': '#C0C0C0'}, 'border': {'color': 'white', 'width': 2, 'height': 0.5}}]

    df_pie_timestamp['Order'] = df_pie_timestamp['Tipo_de_ocorrencia'].map(order)
    df_pie_timestamp = df_pie_timestamp.sort_values(by='Order')
    df_pie_timestamp['Tipo_de_ocorrencia'] = df_pie_timestamp['Tipo_de_ocorrencia'].str.capitalize()



    df_pie_final_period = SQLRepository.repren_tipos_ocorrencia_final_period(escopo, data_fim)
    df_pie_final_period['Order'] = df_pie_final_period['Tipo_de_ocorrencia'].map(order)
    df_pie_final_period = df_pie_final_period.sort_values(by='Order')
    categorias_esperadas = ['CRÍTICA', 'ELOGIO', 'INFORMAÇÃO', 'RECLAMAÇÃO', 'SOLICITAÇÃO', 'SUGESTÃO',
                            'COMENTÁRIOS E MENÇÕES', 'OUTRAS INTERAÇÕES']

    categorias_presentes = df_pie_final_period['Tipo_de_ocorrencia'].unique()
    novos_points = []
    for i, categoria in enumerate(categorias_esperadas):
        if categoria in categorias_presentes:
            novos_points.append(points[i])



    df_pie_timestamp['Tipo_de_ocorrencia'] = df_pie_timestamp['Tipo_de_ocorrencia'].str.title()

    df_pie_final_period['Tipo_de_ocorrencia'] = df_pie_final_period['Tipo_de_ocorrencia'].str.title()

    total_manifestações = SQLRepository.get_total_manifestation_bd(escopo, data_inicio_format, data_fim_format)
    soma_total_geral = int(total_manifestações['Total'].sum())
    sum_period_data = int(df_pie_timestamp['Total'].sum())
    sum_final_data = int(df_pie_final_period['Total'].sum())


    df_acumulate_line = SQLRepository.chart_columns_line_reclamacao(escopo, data_inicio, data_fim)
    df_acumulate_line['Tipo_de_ocorrencia'] = df_acumulate_line['Tipo_de_ocorrencia'].str.title()
    total_reclamacoes_acumulate = df_acumulate_line['Total'].sum()
    df_acumulate_line = df_acumulate_line.sort_values(by='Total', ascending=False)
    volume_menor = df_acumulate_line[
    df_acumulate_line['Tipo_de_ocorrencia'] == 'Manifestações Em Menor Volume']
    resto_acumulate = df_acumulate_line[
    df_acumulate_line['Tipo_de_ocorrencia'] != 'Manifestações Em Menor Volume']
    df_acumulate_line = pd.concat([resto_acumulate, volume_menor])
    df_acumulate_line['% Acumulado'] = df_acumulate_line['Total'] / total_reclamacoes_acumulate
    df_acumulate_line['% Acumulado'] = df_acumulate_line['% Acumulado'].cumsum()





    df_acumulate_line_date_final = SQLRepository.chart_columns_line_reclamacao_last_date(escopo, data_fim)
    df_acumulate_line_date_final['Tipo_de_ocorrencia'] = df_acumulate_line_date_final['Tipo_de_ocorrencia'].str.title()
    total_acumulate_final_date = df_acumulate_line_date_final['Total'].sum()
    df_acumulate_line_date_final = df_acumulate_line_date_final.sort_values(by='Total', ascending=False)
    volume_menor_final = df_acumulate_line_date_final[df_acumulate_line_date_final['Tipo_de_ocorrencia'] == 'Manifestações Em Menor Volume']
    resto_acumulate_final = df_acumulate_line_date_final[df_acumulate_line_date_final['Tipo_de_ocorrencia'] != 'Manifestações Em Menor Volume']
    df_acumulate_line_date_final = pd.concat([resto_acumulate_final, volume_menor_final])
    df_acumulate_line_date_final['% Acumulado'] = df_acumulate_line_date_final['Total'] / total_acumulate_final_date
    df_acumulate_line_date_final['% Acumulado'] = df_acumulate_line_date_final['% Acumulado'].cumsum()



    df_except_reclamacao = SQLRepository.chart_columns_line_except_reclamacao(escopo, data_inicio, data_fim)
    df_except_reclamacao['Tipo_de_ocorrencia'] = df_except_reclamacao['Tipo_de_ocorrencia'].str.title()
    total_except_reclamacao = df_except_reclamacao['Total'].sum()
    # Ordenar o DataFrame pela coluna 'Total' em ordem decrescente
    df_except_reclamacao = df_except_reclamacao.sort_values(by='Total', ascending=False)
    # Separar a categoria "Manifestações Em Menor Volume"
    menor_volume = df_except_reclamacao[df_except_reclamacao['Tipo_de_ocorrencia'] == 'Manifestações Em Menor Volume']
    resto = df_except_reclamacao[df_except_reclamacao['Tipo_de_ocorrencia'] != 'Manifestações Em Menor Volume']
    # Concatenar as partes, garantindo que "Manifestações Em Menor Volume" fique no final
    df_except_reclamacao = pd.concat([resto, menor_volume])
    # Calcular a coluna '% Acumulado'
    df_except_reclamacao['% Acumulado'] = df_except_reclamacao['Total'] / total_except_reclamacao
    df_except_reclamacao['% Acumulado'] = df_except_reclamacao['% Acumulado'].cumsum()


    df_except_reclamacao_last_date = SQLRepository.chart_columns_line_except_reclamacao_last_date(escopo, data_fim)
    df_except_reclamacao_last_date['Tipo_de_ocorrencia'] = df_except_reclamacao['Tipo_de_ocorrencia'].str.title()
    total_except_reclamacao_last_date = df_except_reclamacao_last_date['Total'].sum()
    df_except_reclamacao_last_date = df_except_reclamacao_last_date.sort_values(by='Total', ascending=False)
    menor_volume_last_date = df_except_reclamacao_last_date[df_except_reclamacao_last_date['Tipo_de_ocorrencia'] == 'Manifestações Em Menor Volume']
    resto_last_date = df_except_reclamacao_last_date[df_except_reclamacao_last_date['Tipo_de_ocorrencia'] != 'Manifestações Em Menor Volume']
    df_except_reclamacao_last_date = pd.concat([resto_last_date, menor_volume_last_date])
    df_except_reclamacao_last_date['% Acumulado'] = df_except_reclamacao_last_date['Total'] / total_except_reclamacao_last_date
    df_except_reclamacao_last_date['% Acumulado'] = df_except_reclamacao_last_date['% Acumulado'].cumsum()




    df_per_categoria = SQLRepository.total_per_category(escopo, data_inicio, data_fim)
    df_per_categoria['SubCategoria'] = df_per_categoria['SubCategoria'].str.title()
    df_per_categoria = df_per_categoria.sort_values(by=df_per_categoria.columns[1], ascending=False)
    df_per_categoria = df_per_categoria.fillna(0)


    df_per_categoria_final_date = SQLRepository.total_per_category_period(escopo,data_fim)
    df_per_categoria_final_date = df_per_categoria_final_date.sort_values(by=df_per_categoria_final_date.columns[1], ascending=False)
    df_per_categoria_final_date['SubCategoria'] = df_per_categoria_final_date['SubCategoria'].str.title()
    df_per_categoria_final_date = df_per_categoria_final_date.fillna(0)



    desktop_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    if not os.path.exists(desktop_path):  # Caso o usuário não esteja no Windows
        desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')

    reports_folder_path = os.path.join(desktop_path, 'Graficos_Py')
    if not os.path.exists(reports_folder_path):
        os.makedirs(reports_folder_path)

    output_file_path = os.path.join(reports_folder_path, 'output.xlsx')
    name_graph = f'Gráficos_p_Relátorio {titulo_grafico} - {data_inicio} a {data_fim}.xlsx'
    file_path = os.path.join(reports_folder_path, name_graph)

    with pd.ExcelWriter(output_file_path) as writer:
        df_evolucao_p_canal.to_excel(writer, sheet_name=f'Manifestação_p_Canal {data_inicio}', index=False)
        df_pie_timestamp.to_excel(writer, sheet_name='Manis_Tipo_Ocorrencia', index=False)
        df_pie_final_period.to_excel(writer, sheet_name=f'Manis_Tipo_Ocorrencia {data_fim}', index=False)
        df_acumulate_line.to_excel(writer, sheet_name='% acumulado', index=False)
        df_acumulate_line_date_final.to_excel(writer, sheet_name=f'% Acumulado {data_fim}', index=False)
        df_except_reclamacao.to_excel(writer, sheet_name='Exceto reclamacao', index=False)
        df_except_reclamacao_last_date.to_excel(writer, sheet_name=f'Exceto reclamacao {data_fim}', index=False)
        df_per_categoria.to_excel(writer, sheet_name='Por Categoria', index=False)
        df_per_categoria_final_date.to_excel(writer, sheet_name=f'Por Categoria {data_fim}', index=False)

    dataframe_original = pd.read_excel(output_file_path, sheet_name=None)
    workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})

    name_graph = f'Gráficos_p_Relátorio{data_inicio}.xlsx'
    file_path = os.path.join(reports_folder_path, name_graph)

    for sheet_name, df in dataframe_original.items():
        worksheet = workbook.add_worksheet(sheet_name)
        for row_num, row_data in enumerate(df.values):
            worksheet.write_row(row_num + 1, 0, row_data)
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value)

        if sheet_name == f'Manifestação_p_Canal {data_inicio}':
            soma_tradicional = df['Quantidade_Linhas_tradicional'].sum()
            soma_digital = df['Quantidade_Linhas_digital'].sum()
            df.rename(columns={df.columns[1]: f'Tradicionais = {soma_tradicional} ',
                               df.columns[2]: f'Digitais = {soma_digital}'}, inplace=True)
            date_format = workbook.add_format({'num_format': 'mmm/yyyy'})
            worksheet.set_column('A:A', None, date_format)
            chart_sheet = workbook.add_chartsheet('GRAPH01')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'line'})

            for i in range(1, df.shape[1]):
                series_name = df.columns[i]
                cor = '#800080' if series_name == f'Digitais = {soma_digital}' else '#DAA520'
                chart.add_series({
                    'name': series_name,
                    'categories': [sheet_name, 1, 0, df.shape[0], 0],
                    'values': [sheet_name, 1, i, df.shape[0], i],
                    'line': {'color': cor, 'width': 1.0},
                    'marker': {'type': 'circle', 'border': {'color': cor}, 'fill': {'color': cor}},
                    'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12, 'color': cor}, 'position': 'above', 'align': 'left'}})

            chart.set_x_axis({'num_font': {'name': 'Segoe UI', 'size': 12, 'rotation': -45}})
            chart.set_title({
                'name': f'{titulo_grafico} \nEvolução das manifestações por canal\n{soma_total_geral} manifestações - {data_inicio_formatada} a {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}})
            chart.set_y_axis({'major_gridlines': {'visible': False}, 'visible': False})
            chart.set_plotarea({'border': {'none': True}, 'fill': {'none': True}})
            chart.set_legend({'position': 'bottom',
                              'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': 'black'}})
            altura_cm = 16.66
            largura_cm = 26.77
            chart.set_size({'width': largura_cm, 'height': altura_cm})
            chart_sheet.set_chart(chart)

        elif sheet_name == 'Manis_Tipo_Ocorrencia':
            chart_sheet = workbook.add_chartsheet('GRAPH02')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'pie'})
            chart.add_series({
                'name': 'Ocorrências',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'data_labels': {'percentage': True, 'position': 'outside_end', 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'points': points})
            # Definir título do gráfico
            chart.set_title({
                'name': f'{titulo_grafico} \nRepresentatividade de tipos de ocorrência\n{sum_period_data} manifestações - {data_inicio_formatada} a {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'},
                'overlay': True})
            chart.set_legend({
                'position': 'left',  # Define a posição da legenda
                'font': {'name': 'Segoe UI', 'size': 14, 'color': 'black'}})

            chart_sheet.set_chart(chart)

        elif sheet_name == f'Manis_Tipo_Ocorrencia {data_fim}':
            chart_sheet = workbook.add_chartsheet('GRAPH03')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'pie'})
            chart.add_series({
                'name': 'Ocorrências',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'data_labels': {'percentage': True, 'position': 'outside_end', 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'points': novos_points})
            # Definir título do gráfico
            chart.set_title({
                'name': f'{titulo_grafico} \nRepresentatividade de tipos de ocorrência\n{sum_final_data} manifestações - {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'},
                'overlay': True
                            })
            chart.set_legend({
                'position': 'left',  # Define a posição da legenda
                'font': {'name': 'Segoe UI', 'size': 14, 'color': 'black'}})
            chart_sheet.set_chart(chart)

        elif sheet_name == '% acumulado':
            chart_sheet = workbook.add_chartsheet('GRAPH04')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'column'})
            chart.add_series({
                'name': 'Reclamações',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'fill': {'color': '#FF0000'},
                'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12}},
                'gap': 50
            })
            chart_sheet.set_chart(chart)
            chart.set_title(
                {'name': f'{titulo_grafico} \n10 principais motivos de reclamações \n{total_reclamacoes_acumulate} reclamações - {data_inicio_formatada} a {data_fim_formatada}',
                 'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}})
            chart.set_legend({'position': 'bottom', 'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': 'black'}})
            chart.set_x_axis({'num_font': {'name': 'Segoe UI', 'size': 12, 'rotation': -66}, 'major_gridlines': {'visible': False},
                              'line': {'none': True}})
            chart.set_y_axis({
                'min': 0,
                'max': total_reclamacoes_acumulate,
                'major_gridlines': {'visible': False, 'line': {'color': '#FFFFFF'}},
                'minor_gridlines': {'visible': False, 'line': {'color': '#FFFFFF'}},
                'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': '#FFFFFF'},
                'num_font': {'color': 'white'},
                'line': {'none': True}
            })

            line_chart = workbook.add_chart({'type': 'line'})
            line_chart.add_series({
                'name': '% Acumulado',
                'categories': [sheet_name, 1, 0, df_acumulate_line.shape[0], 0],
                'values': [sheet_name, 1, 2, df_acumulate_line.shape[0], 2],
                'line': {'color': 'black', 'width': 1.0},
                'y2_axis': True,
            })
            line_chart.set_y2_axis({
                'line': {'none': True},
                'major_gridlines': {'visible': False},
                'minor_gridlines': {'visible': False},
                'num_format': '0%',
                'max': 1,
                'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': 'black'}
            })
            # Combinar os gráficos de colunas e linha
            chart.combine(line_chart)

        elif sheet_name == f'% Acumulado {data_fim}':
            chart_sheet = workbook.add_chartsheet('GRAPH05')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'column'})
            chart.add_series({
                'name': 'Reclamações',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'fill': {'color': '#FF0000'},
                'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'gap': 50
            })
            chart_sheet.set_chart(chart)
            chart.set_title(
                {'name': f'{titulo_grafico} \n10 principais motivos de reclamações \n{total_acumulate_final_date} reclamações - {data_fim_formatada}',
                    'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}})
            chart.set_legend({'position': 'bottom',
                              'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False,
                                       'color': 'black'}})
            chart.set_x_axis(
                {'num_font': {'name': 'Segoe UI', 'size': 12, 'rotation': -66}, 'major_gridlines': {'visible': False},
                 'line': {'none': True}})


            chart.set_y_axis({
                'min': 0,
                'max': total_acumulate_final_date,
                'major_gridlines': {'visible': False, 'line': {'color': '#FFFFFF'}},
                'minor_gridlines': {'visible': False, 'line': {'color': '#FFFFFF'}},
                'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': '#FFFFFF'},
                'num_font': {'color': 'white'},
                'line': {'none': True}
            })

            line_chart = workbook.add_chart({'type': 'line'})
            line_chart.add_series({
                'name': '% Acumulado',
                'categories': [sheet_name, 1, 0, df_acumulate_line_date_final.shape[0], 0],
                'values': [sheet_name, 1, 2, df_acumulate_line_date_final.shape[0], 2],
                'line': {'color': 'black', 'width': 1.0},
                'y2_axis': True,
            })
            line_chart.set_y2_axis({
                'major_gridlines': {'visible': False},
                'minor_gridlines': {'visible': False},
                'num_format': '0%',
                'max': 1,
                'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': 'black'},
                'line': {'none': True}
            })
            # Combinar os gráficos de colunas e linha
            chart.combine(line_chart)
        elif sheet_name == 'Exceto reclamacao':
            chart_sheet = workbook.add_chartsheet('GRAPH06')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'column'})

            # Definindo cores para as categorias antes do hífen
            cores = {
                'Redes Sociais': '#008080',
                'Elogio': '#32CD32',
                'Solicitação': '#F4A460',
                'Informação': '#6495ED',
                'Manifestações Em Menor Volume': '#4F4F4F',
                'Crítica': '#FFD700',
                'Sugestão': '#FF00FF'
            }

            categorias = df.iloc[:, 0].tolist()  # Lista de todas as categorias completas
            valores = df.iloc[:, 1].tolist()  # Lista de todos os valores correspondentes

            # Criando lista de cores para cada ponto
            cores_pontos = []
            for categoria_completa in categorias:
                categoria = categoria_completa.split(' - ')[0]
                cor = cores.get(categoria, '#4F4F4F')  # Define a cor com base na categoria
                cores_pontos.append({'fill': {'color': cor}})

            # Adicionar a série com valores e cores
            chart.add_series({
                'name': 'Manifestações',
                'categories': [sheet_name, 1, 0, len(categorias), 0],
                'values': [sheet_name, 1, 1, len(categorias), 1],
                'points': cores_pontos,
                'fill': {'color': '#4F4F4F'},
                'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'gap': 50
            })


            # Definir valor máximo do eixo Y para o gráfico de colunas
            chart.set_y_axis({
                'min': 0,
                'max': total_except_reclamacao,
                'major_gridlines': {'visible': False, 'line': {'color': '#FFFFFF'}},
                'minor_gridlines': {'visible': False, 'line': {'color': '#FFFFFF'}},
                'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': '#FFFFFF'},
                'num_font': {'color': 'white'},
                'line': {'none': True}
            })

            chart_sheet.set_chart(chart)
            chart.set_title({
                'name': f'{titulo_grafico} \n10 principais manifestações (exceto reclamações) \n{total_except_reclamacao} manifestações - {data_inicio_formatada} - {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}
            })
            chart.set_legend({'position': 'bottom',
                              'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False,
                                       'color': 'black'},})
            chart.set_x_axis({
                'num_font': {'name': 'Segoe UI', 'size': 12, 'rotation': -66},
                'major_gridlines': {'visible': False},
                'line': {'none': True}
            })

            line_chart = workbook.add_chart({'type': 'line'})
            line_chart.add_series({
                'name': '% Acumulado',
                'categories': [sheet_name, 1, 0, df_except_reclamacao.shape[0], 0],
                'values': [sheet_name, 1, 2, df_except_reclamacao.shape[0], 2],
                'line': {'color': 'black', 'width': 1.0},
                'y2_axis': True,
            })
            line_chart.set_y2_axis({
                'major_gridlines': {'visible': False, 'line': {'color': 'white'}},
                'minor_gridlines': {'visible': False, 'line': {'color': 'white'}},
                'num_format': '0%',
                'max': 1,
                'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': 'black'},
                'line': {'none': True}
            })

            # Combinar os gráficos de colunas e linha
            chart.combine(line_chart)

        elif sheet_name == f'Exceto reclamacao {data_fim}':
            chart_sheet = workbook.add_chartsheet('GRAPH07')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'column'})
            cores = {
                'Redes Sociais': '#008080',
                'Elogio': '#32CD32',
                'Solicitação': '#F4A460',
                'Informação': '#6495ED',
                'Manifestações Em Menor Volume': '#4F4F4F',
                'Crítica': '#FFD700',
                'Sugestão': '#FF00FF'
            }

            categorias = df.iloc[:, 0].tolist()  # Lista de todas as categorias completas
            valores = df.iloc[:, 1].tolist()  # Lista de todos os valores correspondentes

            # Criando lista de cores para cada ponto
            cores_pontos = []
            for categoria_completa in categorias:
                categoria = categoria_completa.split(' - ')[0]
                cor = cores.get(categoria, '#4F4F4F')  # Define a cor com base na categoria
                cores_pontos.append({'fill': {'color': cor}})

            # Adicionar a série com valores e cores
            chart.add_series({
                'name': 'Manifestações',
                'categories': [sheet_name, 1, 0, len(categorias), 0],
                'values': [sheet_name, 1, 1, len(categorias), 1],
                'points': cores_pontos,
                'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'gap': 50,
                'fill': {'color': '#4F4F4F'},
            })

            chart_sheet.set_chart(chart)
            chart.set_title({
                'name': f'{titulo_grafico} \n10 principais manifestações (exceto reclamações) \n{total_except_reclamacao_last_date} manifestações - {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}
            })
            chart.set_legend({'position': 'bottom',
                              'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False,
                                       'color': 'black'}})
            chart.set_x_axis({
                'num_font': {'name': 'Segoe UI', 'size': 12, 'rotation': -66},
                'major_gridlines': {'visible': False},
                'line': {'none': True}
            })

            chart.set_y_axis({
                    'min': 0,
                    'max': total_except_reclamacao_last_date,
                    'major_gridlines': {'visible': False, 'line': {'color': 'white'}},
                    'minor_gridlines': {'visible': False, 'line': {'color': 'white'}},
                    'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': 'white'},
                    'num_font': {'color': 'white'},
                    'line': {'none': True}
                })


            line_chart = workbook.add_chart({'type': 'line'})
            line_chart.add_series({
                'name': '% Acumulado',
                'categories': [sheet_name, 1, 0, len(categorias), 0],
                # Ajuste para o mesmo intervalo de categorias do gráfico de colunas
                'values': [sheet_name, 1, 2, len(categorias), 2],
                # Ajuste para os mesmos valores e colunas correspondentes
                'line': {'color': 'black', 'width': 1.0},
                'y2_axis': True,
            })
            line_chart.set_y2_axis({
                'major_gridlines': {'visible': False},
                'minor_gridlines': {'visible': False},
                'num_format': '0%',
                'max': 1,
                'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False, 'color': 'black'},
                'line': {'none': True}
            })
            # Combinar os gráficos de colunas e linha
            chart.combine(line_chart)

        elif sheet_name == 'Por Categoria':
            chart_sheet = workbook.add_chartsheet('GRAPH08')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})

            for i, col in enumerate(df.columns[1:], 1):
                chart.add_series({
                    'name': [sheet_name, 0, i],
                    'categories': [sheet_name, 1, 0, len(df), 0],
                    'values': [sheet_name, 1, i, len(df), i],
                    'fill': {'color': ['#9DC3E6', '#BFBFBF', '#FFD966'][i - 1]},
                    'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12}},
                    'gap': 50
                })

            chart.set_title({'name': f'{titulo_grafico} \nTotal de manifestações por categoria \n{data_inicio_formatada} a {data_fim_formatada}',
                             'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}})
            chart.set_x_axis({'major_gridlines': {'visible': False},
                'minor_gridlines': {'visible': False},
                              'num_font': {'name': 'Segoe UI', 'size': 12}, 'major_gridlines': {'visible': False},
                              'line': {'none': True}})
            chart.set_y_axis({'visible': False,
                              'major_gridlines': {'visible': False}})
            chart.set_legend({'position': 'top',
                                          'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False,
                                                   'color': 'black'}})
            chart.set_size({'x_scale': 1.5, 'y_scale': 1.5})
            chart_sheet.set_chart(chart)

        elif sheet_name == f'Por Categoria {data_fim}':
            chart_sheet = workbook.add_chartsheet('GRAPH09')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})

            for i, col in enumerate(df.columns[1:], 1):
                chart.add_series({
                    'name': [sheet_name, 0, i],
                    'categories': [sheet_name, 1, 0, len(df), 0],
                    'values': [sheet_name, 1, i, len(df), i],
                    'fill': {'color': ['#9DC3E6', '#BFBFBF', '#FFD966'][i - 1]},
                    'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12}},
                    'gap': 50})

            chart.set_title({'name': f'{titulo_grafico} \nTotal de manifestações por categoria \n{data_fim_formatada}',
                             'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}})
            chart.set_x_axis({'major_gridlines': {'visible': False},
                            'minor_gridlines': {'visible': False},
                              'num_font': {'name': 'Segoe UI', 'size': 12}, 'major_gridlines': {'visible': False},
                              'line': {'none': True}})
            chart.set_y_axis({'visible': False,
                              'major_gridlines': {'visible': False}})
            chart.set_legend({'position': 'top',
                                          'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False,
                                                   'color': 'black'}})
            chart_sheet.set_chart(chart)



    workbook.close()

    messagebox.showinfo("Concluído", f"Os gráficos foram executados com sucesso e os resultados foram salvos no arquivo {file_path}")

# Criando a janela principal
root = tk.Tk()
root.title("Gerador de Gráficos Maena")
root.geometry("400x300")

# Criando os widgets
label_escopo = tk.Label(root, text="Informe o número do escopo desejado:")
label_escopo.pack()
entry_escopo = tk.Entry(root)
entry_escopo.pack()

label_escopo_tradicional = tk.Label(root, text="Informe o Número do escopo referente ao canal TRADICIONAL:")
label_escopo_tradicional.pack()
entry_escopo_tradicional = tk.Entry(root)
entry_escopo_tradicional.pack()

label_escopo_digital = tk.Label(root, text="Informe o número do escopo referente ao canal DIGITAL:")
label_escopo_digital.pack()
entry_escopo_digital = tk.Entry(root)
entry_escopo_digital.pack()

label_data_inicio = tk.Label(root, text="Informe a data de Partida/Inicial nesse formato -> yyyy-mm:")
label_data_inicio.pack()
entry_data_inicio = tk.Entry(root)
entry_data_inicio.pack()

label_data_fim = tk.Label(root, text="Informe a data de Encerramento/Fim nesse formato -> yyyy-mm:")
label_data_fim.pack()
entry_data_fim = tk.Entry(root)
entry_data_fim.pack()

button_executar = tk.Button(root, text="Gerar Gráficos", command=run_code, bg="#FF69B4")
button_executar.pack(side='bottom', pady=10)

# Iniciar o loop principal da aplicação
root.mainloop()
