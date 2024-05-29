import tkinter as tk
import os
from tkinter import messagebox
from src.repository.SQL.repository import SQLRepository
import xlsxwriter
import pandas as pd
from datetime import datetime

def run_code():
    escopo = entry_escopo.get()
    id_ = entry_id.get()
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
    df_pie_timestamp = SQLRepository.repren_tipos_ocorrencia_timestamp_period(escopo, data_inicio, data_fim)
    df_pie_first_period = SQLRepository.repren_tipos_ocorrencia_first_period(escopo, data_inicio)
    df_pie_final_period = SQLRepository.repren_tipos_ocorrencia_final_period(escopo, data_fim)

    df_pie_timestamp['Tipo_de_ocorrencia'] = df_pie_timestamp['Tipo_de_ocorrencia'].str.title()
    df_pie_first_period['Tipo_de_ocorrencia'] = df_pie_first_period['Tipo_de_ocorrencia'].str.title()
    df_pie_final_period['Tipo_de_ocorrencia'] = df_pie_final_period['Tipo_de_ocorrencia'].str.title()

    total_manifestações = SQLRepository.get_total_manifestation_bd(id_, escopo)
    soma_total_geral = int(total_manifestações['Total'].sum())
    sum_first_data = int(df_pie_first_period['Total'].sum())
    sum_final_data = int(df_pie_final_period['Total'].sum())

    desktop_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    output_file_path = os.path.join(desktop_path, 'output.xlsx')

    with pd.ExcelWriter(output_file_path) as writer:
        df_evolucao_p_canal.to_excel(writer, sheet_name=f'Manifestação_p_Canal {data_inicio}', index=False)
        df_pie_timestamp.to_excel(writer, sheet_name='Manis_Tipo_Ocorrencia', index=False)
        df_pie_first_period.to_excel(writer, sheet_name=f'Manis_Tipo_Ocorrencia {data_inicio}', index=False)
        df_pie_final_period.to_excel(writer, sheet_name=f'Manis_Tipo_Ocorrencia {data_fim}', index=False)

    dataframe_original = pd.read_excel(output_file_path, sheet_name=None)
    name_graph = f'Gráficos_p_Relátorio{data_inicio}.xlsx'
    file_path = os.path.join(desktop_path, name_graph)
    workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})
    chart_title_format = workbook.add_format({'bold': True, 'font_size': 14})

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
                    'data_labels': {'value': True, 'font': {'color': cor}, 'position': 'above', 'align': 'left'}})

            chart.set_title({
                'name': f'{titulo_grafico} \nEvolução das manifestações por canal\n{soma_total_geral} manifestações - {data_inicio_formatada} a {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 14, 'bold': False, 'color': 'black'}})
            chart.set_y_axis({'major_gridlines': {'visible': False}, 'visible': False})
            chart.set_plotarea({'border': {'none': True}, 'fill': {'none': True}})
            chart.set_legend({'position': 'bottom'})
            chart.set_size({'x_scale': 2, 'y_scale': 1.5})
            chart_sheet.set_chart(chart)

        elif sheet_name == 'Manis_Tipo_Ocorrencia':
            num_rows = df.shape[0]
            chart_sheet = workbook.add_chartsheet('Graf_manifestacao_p_ocorrencia')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'pie'})
            width_cm = 17
            height_cm = 12
            dpi = 96
            width_px = width_cm * dpi / 2.54
            height_px = height_cm * dpi / 2.54
            chart.set_size({'width': width_px, 'height': height_px})
            categorias = df['Tipo_de_ocorrencia'].tolist()
            points = [
                {'fill': {'color': '#FFFF00'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#32CD32'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#87CEFA'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#C0C0C0'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': 'red'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#F4A460'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#FF00FF'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#008080'}, 'border': {'color': 'white', 'width': 2}}]

            chart.add_series({
                'name': 'Ocorrências',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'data_labels': {'percentage': True, 'position': 'outside_end', 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'points': points})
            # Definir título do gráfico
            chart.set_title({
                'name': f'{titulo_grafico} \nRepresentatividade de tipos de ocorrência\n{soma_total_geral} manifestações - {data_inicio_formatada} a {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 14, 'bold': False, 'color': 'black'},
                'overlay': True})
            chart.set_legend({
                'position': 'left',  # Define a posição da legenda
                'font': {'name': 'Segoe UI (Corpo)', 'size': 16, 'color': 'black'}})
            chart.set_size({'x_scale': 2, 'y_scale': 1.5})  # Ajusta o tamanho do gráfico
            chart_sheet.set_chart(chart)

        elif sheet_name == f'Manis_Tipo_Ocorrencia {data_inicio}':
            num_rows = df.shape[0]
            chart_sheet = workbook.add_chartsheet(f'Graf_Ocorrencia {data_inicio}')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'pie'})
            width_cm = 17
            height_cm = 12
            dpi = 96
            width_px = width_cm * dpi / 2.54
            height_px = height_cm * dpi / 2.54
            chart.set_size({'width': width_px, 'height': height_px})
            categorias = df['Tipo_de_ocorrencia'].tolist()
            points = [
                {'fill': {'color': '#FFFF00'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#32CD32'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#87CEFA'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#C0C0C0'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': 'red'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#F4A460'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#FF00FF'}, 'border': {'color': 'white', 'width': 2}},
                {'fill': {'color': '#008080'}, 'border': {'color': 'white', 'width': 2}}]
            chart.add_series({
                'name': 'Ocorrências',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'data_labels': {'percentage': True, 'position': 'outside_end', 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'points': points})
            # Definir título do gráfico
            chart.set_title({
                'name': f'{titulo_grafico} \nRepresentatividade de tipos de ocorrência\n{sum_first_data} manifestações - {data_inicio_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 14, 'bold': False, 'color': 'black'},
                'overlay': True})
            chart.set_legend({
                'position': 'left',  # Define a posição da legenda
                'font': {'name': 'Segoe UI (Corpo)', 'size': 16, 'color': 'black'}})
            chart.set_size({'x_scale': 2, 'y_scale': 1.5})  # Ajusta o tamanho do gráfico
            chart_sheet.set_chart(chart)

        elif sheet_name == f'Manis_Tipo_Ocorrencia {data_fim}':
            num_rows = df.shape[0]
            chart_sheet = workbook.add_chartsheet(f'Graf_Ocorrencia {data_fim}')
            chart_sheet.set_tab_color('#32CD32')
            chart = workbook.add_chart({'type': 'pie'})
            width_cm = 17
            height_cm = 12
            dpi = 96
            width_px = width_cm * dpi / 2.54
            height_px = height_cm * dpi / 2.54
            chart.set_size({'width': width_px, 'height': height_px})
            categorias = df['Tipo_de_ocorrencia'].tolist()
            cores = ['#FFFF00', '#32CD32', '#87CEFA', '#C0C0C0', 'red', '#F4A460', '#FF00FF', '#008080']
            mapeamento_cores = {categoria: cor for categoria, cor in zip(categorias, cores)}
            chart.add_series({
                'name': 'Ocorrências',
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, 1, df.shape[0], 1],
                'data_labels': {'percentage': True, 'position': 'outside_end', 'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
                'points':[{'fill': {'color': mapeamento_cores[categoria]}} for categoria in categorias]})
            # Definir título do gráfico
            chart.set_title({
                'name': f'{titulo_grafico} \nRepresentatividade de tipos de ocorrência\n{sum_final_data} manifestações - {data_fim_formatada}',
                'name_font': {'name': 'Segoe UI', 'size': 14, 'bold': False, 'color': 'black'},
                'overlay': True
                            })
            chart.set_legend({
                'position': 'left',  # Define a posição da legenda
                'font': {'name': 'Segoe UI (Corpo)', 'size': 16, 'color': 'black'}})
            chart.set_size({'x_scale': 2, 'y_scale': 1.5})  # Ajusta o tamanho do gráfico
            chart_sheet.set_chart(chart)

    workbook.close()

    messagebox.showinfo("Concluído", f"Os gráficos foram executados com sucesso e os resultados foram salvos no arquivo {file_path}")

# Criando a janela principal
root = tk.Tk()
root.title("Interface para o Código")
root.geometry("400x400")

# Criando os widgets
label_escopo = tk.Label(root, text="Informe o número do escopo desejado:")
label_escopo.pack()
entry_escopo = tk.Entry(root)
entry_escopo.pack()

label_id = tk.Label(root, text="Informe o número do ID para o escopo escolhido:")
label_id.pack()
entry_id = tk.Entry(root)
entry_id.pack()

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

button_executar = tk.Button(root, text="Gerar Graficos", command=run_code)
button_executar.pack()

# Iniciar o loop principal da aplicação
root.mainloop()
