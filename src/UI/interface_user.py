# third party
import tkinter as tk
from tkinter import messagebox
import xlsxwriter
import pandas as pd
# standard
import os
from datetime import datetime
# local
from src.models.variables.variables import VariableTreatment
from src.models.style_dfs.style import StylesDfs
from src.domain.services.charts import DomainService



def run_code():
    escopo = entry_escopo.get()
    escopo_trad = entry_escopo_tradicional.get()
    escopo_dig = entry_escopo_digital.get()
    start_date = entry_data_inicio.get()
    end_date = entry_data_fim.get()

    title_chart = VariableTreatment.chart_title_escopo(escopo)
    start_date_sql = datetime.strptime(start_date, "%Y-%m")
    end_date_sql = datetime.strptime(end_date, "%Y-%m")
    date_formatting_initial = VariableTreatment.date_formatting_initial(start_date)
    date_formatting_end = VariableTreatment.date_formatting_end(end_date)
    sum_total = VariableTreatment.get_sum_total(escopo, start_date_sql, end_date_sql)
    sum_period_data = VariableTreatment.get_sum_timestamp_chart_pie(escopo, start_date, end_date)
    sum_first_data = VariableTreatment.get_sum_first_period_chart_pie(escopo, start_date)
    sum_final_data = VariableTreatment.get_sum_end_period_chart_pie(escopo, end_date)
    points = StylesDfs.points_chart_pie

    desktop_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    if not os.path.exists(desktop_path):  # Caso o usuário não esteja no Windows
        desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')

    reports_folder_path = os.path.join(desktop_path, 'Graficos_Py')
    if not os.path.exists(reports_folder_path):
        os.makedirs(reports_folder_path)

    output_file_path = os.path.join(reports_folder_path, 'output.xlsx')
    name_graph = f'Gráficos_p_Relátorio {title_chart} - {start_date} a {end_date}.xlsx'
    file_path = os.path.join(reports_folder_path, name_graph)

    with (pd.ExcelWriter(output_file_path) as writer):
        VariableTreatment.date_order_channel(escopo_trad, escopo_dig, start_date, end_date).to_excel(writer, sheet_name=f'Manifestação_p_Canal {start_date}', index=False)
        VariableTreatment.order_occurrences_chart_pie(escopo, start_date, end_date).to_excel(writer, sheet_name='Manis_Tipo_Ocorrencia', index=False)
        VariableTreatment.order_chart_pie_first_period(escopo, start_date).to_excel(writer, sheet_name=f'Manis_Tipo_Ocorrencia {start_date}', index=False)
        VariableTreatment.order_chart_pie_final_period(escopo, end_date).to_excel(writer, sheet_name=f'Manis_Tipo_Ocorrencia {end_date}', index=False)

    original_df = pd.read_excel(output_file_path, sheet_name=None)
    workbook = xlsxwriter.Workbook(file_path, {'nan_inf_to_errors': True})

    name_graph = f'Gráficos_p_Relátorio - {title_chart} - {start_date} a {end_date}.xlsx'
    file_path = os.path.join(reports_folder_path, name_graph)

    for sheet_name, df in original_df.items():
        worksheet = workbook.add_worksheet(sheet_name)
        for row_num, row_data in enumerate(df.values):
            worksheet.write_row(row_num + 1, 0, row_data)
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value)

        if sheet_name == f'Manifestação_p_Canal {start_date}':
            DomainService.chart_line_channel(df=df, workbook=workbook, worksheet=worksheet, sheet_name=sheet_name, title_chart=title_chart, sum_total=sum_total, date_start_format=date_formatting_initial, date_end_format=date_formatting_end)
        elif sheet_name == 'Manis_Tipo_Ocorrencia':
            DomainService.chart_pie_types_occurrences_period(workbook=workbook, sheet_name=sheet_name, df=df, points=points, title_chart=title_chart, sum_period_data=sum_period_data, date_start_format=date_formatting_initial, date_end_format=date_formatting_end)
        elif sheet_name == f'Manis_Tipo_Ocorrencia {start_date}':
            DomainService.chart_pie_occurrences_first_period(workbook=workbook, sheet_name=sheet_name, df=df, points=points, date_start_format=start_date, sum_first_data=sum_first_data, title_chart=title_chart)
        elif sheet_name == f'Manis_Tipo_Ocorrencia {end_date}':
            DomainService.chart_pie_occurrences_final_period(workbook=workbook, sheet_name=sheet_name, df=df, points=points, title_chart=title_chart, date_end_format=end_date, sum_final_data=sum_final_data)

    workbook.close()
    messagebox.showinfo("Concluído", f"Os gráficos foram executados com sucesso e os resultados foram salvos no arquivo - {file_path}")

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

root.mainloop()