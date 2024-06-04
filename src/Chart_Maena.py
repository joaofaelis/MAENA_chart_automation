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
    nome_do_escopo = escopo_name['Nome_Escopo_Hierarquia'].to_string(index=False)
    titulo_grafico = nome_do_escopo.replace('[', '').replace(']', '')

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
    df_pie_timestam_one = df_pie_timestamp.sort_values(by='Order')
    df_pie_timestam_one['Tipo_de_ocorrencia'] = df_pie_timestam_one['Tipo_de_ocorrencia'].str.capitalize()
    df_pie_timestamp_office = df_pie_timestam_one.drop(columns=['Order'])

    df_pie_first_period = SQLRepository.repren_tipos_ocorrencia_first_period(escopo, data_inicio)

    df_pie_first_period['Order'] = df_pie_first_period['Tipo_de_ocorrencia'].map(order)
    df_pie_time_first = df_pie_first_period.sort_values(by='Order')
    df_pie_time_first['Tipo_de_ocorrencia'] =df_pie_time_first['Tipo_de_ocorrencia'].str.capitalize()
    df_pie_timestamp_first = df_pie_time_first.drop(columns=['Order'])

    df_pie_final_period = SQLRepository.repren_tipos_ocorrencia_final_period(escopo, data_fim)

    df_pie_final_period['Order'] = df_pie_final_period['Tipo_de_ocorrencia'].map(order)
    df_pie_time_final = df_pie_final_period.sort_values(by='Order')
    df_pie_time_final['Tipo_de_ocorrencia'] = df_pie_time_final['Tipo_de_ocorrencia'].str.capitalize()
    df_pie_timestamp_final = df_pie_time_final.drop(columns=['Order'])

    df_pie_timestamp['Tipo_de_ocorrencia'] = df_pie_timestamp['Tipo_de_ocorrencia'].str.title()
    df_pie_first_period['Tipo_de_ocorrencia'] = df_pie_first_period['Tipo_de_ocorrencia'].str.title()
    df_pie_final_period['Tipo_de_ocorrencia'] = df_pie_final_period['Tipo_de_ocorrencia'].str.title()

    total_manifestações = SQLRepository.get_total_manifestation_bd(escopo, data_inicio_format, data_fim_format)
    soma_total_geral = int(total_manifestações['Total'].sum())
    sum_period_data = int(df_pie_timestamp['Total'].sum())
    sum_first_data = int(df_pie_first_period['Total'].sum())
    sum_final_data = int(df_pie_final_period['Total'].sum())

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
        df_pie_timestamp_office.to_excel(writer, sheet_name='Manis_Tipo_Ocorrencia', index=False)
        df_pie_timestamp_first.to_excel(writer, sheet_name=f'Manis_Tipo_Ocorrencia {data_inicio}', index=False)
        df_pie_timestamp_final.to_excel(writer, sheet_name=f'Manis_Tipo_Ocorrencia {data_fim}', index=False)

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
            chart_sheet = workbook.add_chartsheet('Graf_Evolucao_canal')
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
            chart_sheet = workbook.add_chartsheet('Graf_manifestacao_p_ocorrencia')
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
            width_in_cm = 10
            height_in_cm = 7
            width_in_pixels = width_in_cm * 37.8
            height_in_pixels = height_in_cm * 37.8

            chart.set_size({'width': width_in_pixels, 'height': height_in_pixels})

            chart_sheet.set_chart(chart)

        elif sheet_name == f'Manis_Tipo_Ocorrencia {data_inicio}':
            num_rows = df.shape[0]
            chart_sheet = workbook.add_chartsheet(f'Graf_Ocorrencia {data_inicio}')

            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'pie'})
            altura_cm = 5
            largura_cm = 5
            largura_pontos = largura_cm * 28.3464567
            altura_pontos = altura_cm * 28.3464567

            # Definir tamanho do gráfico
            chart.set_size({'width': largura_pontos, 'height': altura_pontos})
            chart.add_series({
                'name': 'Ocorrências',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'data_labels': {'percentage': True, 'position': 'outside_end', 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'points': points})
            # Definir título do gráfico
            chart.set_title({
                'name': f'{titulo_grafico} \nRepresentatividade de tipos de ocorrência\n{sum_first_data} manifestações - {data_inicio_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'},
                'overlay': True})
            chart.set_legend({
                'position': 'left',  # Define a posição da legenda
                'font': {'name': 'Segoe UI', 'size': 14, 'color': 'black'}})
            chart_sheet.set_chart(chart)

        elif sheet_name == f'Manis_Tipo_Ocorrencia {data_fim}':
            chart_sheet = workbook.add_chartsheet(f'Graf_Ocorrencia {data_fim}')
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
                'name': f'{titulo_grafico} \nRepresentatividade de tipos de ocorrência\n{sum_final_data} manifestações - {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'},
                'overlay': True
                            })
            chart.set_legend({
                'position': 'left',  # Define a posição da legenda
                'font': {'name': 'Segoe UI', 'size': 14, 'color': 'black'}})
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
