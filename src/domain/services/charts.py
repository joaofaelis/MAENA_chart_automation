
class DomainService:

    @classmethod
    def chart_line_channel(cls, df, workbook, worksheet, sheet_name, title_chart, sum_total, date_start_format, date_end_format):
        sum_trad = df['Quantidade_Linhas_tradicional'].sum()
        sum_digital = df['Quantidade_Linhas_digital'].sum()
        df.rename(columns={df.columns[1]: f'Tradicionais = {sum_trad} ',
                    df.columns[2]: f'Digitais = {sum_digital}'}, inplace=True)

        date_format = workbook.add_format({'num_format': 'mmm/yyyy'})
        worksheet.set_column('A:A', None, date_format)
        chart_sheet = workbook.add_chartsheet('Graf_Evolucao_canal')
        chart_sheet.set_tab_color('#32CD32')
        chart = workbook.add_chart({'type': 'line'})

        for i in range(1, df.shape[1]):
            series_name = df.columns[i]
            cor = '#800080' if series_name == f'Digitais = {sum_digital}' else '#DAA520'
            chart.add_series({
                'name': series_name,
                'categories': [sheet_name, 1, 0, df.shape[0], 0],
                'values': [sheet_name, 1, i, df.shape[0], i],
                'line': {'color': cor, 'width': 1.0},
                'marker': {'type': 'circle', 'border': {'color': cor}, 'fill': {'color': cor}},
                'data_labels': {'value': True, 'font': {'name': 'Segoe UI', 'size': 12, 'color': cor},
                                'position': 'above',
                                'align': 'left'}})

            chart.set_x_axis({'num_font': {'name': 'Segoe UI', 'size': 12, 'rotation': -45}})
            chart.set_title({
                'name': f'{title_chart} \nEvolução das manifestações por canal\n{sum_total} manifestações - {date_start_format} a {date_end_format}',
                'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'}})
            chart.set_y_axis({'major_gridlines': {'visible': False}, 'visible': False})
            chart.set_plotarea({'border': {'none': True}, 'fill': {'none': True}})
            chart.set_legend({'position': 'bottom',
                              'font': {'name': 'Segoe UI', 'size': 12, 'bold': False, 'italic': False,
                                       'color': 'black'}})
            chart_sheet.set_chart(chart)

    @classmethod
    def chart_pie_types_occurrences_period(cls, workbook, sheet_name, df, points, title_chart, sum_period_data, date_start_format, date_end_format):
        chart_sheet = workbook.add_chartsheet('Graf_manifestacao_p_ocorrencia')
        chart_sheet.set_tab_color('#32CD32')
        chart = workbook.add_chart({'type': 'pie'})
        chart.add_series({
            'name': 'Ocorrências',
            'categories': [sheet_name, 1, 0, df.shape[0], 0],
            'values': [sheet_name, 1, 1, df.shape[0], 1],
            'data_labels': {'percentage': True, 'position': 'outside_end',
                            'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
            'points': points})
        chart.set_title({
            'name': f'{title_chart} \nRepresentatividade de tipos de ocorrência\n{sum_period_data} manifestações - {date_start_format} a {date_end_format}',
            'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'},
            'overlay': True})
        chart.set_legend({
            'position': 'left',  # Define a posição da legenda
            'font': {'name': 'Segoe UI', 'size': 14, 'color': 'black'}})
        chart_sheet.set_chart(chart)

    @classmethod
    def chart_pie_occurrences_first_period(cls, workbook, sheet_name, df, points, title_chart, date_start_format, sum_first_data):
        chart_sheet = workbook.add_chartsheet(f'Graf_Ocorrencia {date_start_format}')
        chart_sheet.set_tab_color('#32CD32')
        chart = workbook.add_chart({'type': 'pie'})
        chart.add_series({
            'name': 'Ocorrências',
            'categories': [sheet_name, 1, 0, df.shape[0], 0],
            'values': [sheet_name, 1, 1, df.shape[0], 1],
            'data_labels': {'percentage': True, 'position': 'outside_end',
                            'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
            'points': points})
        # Definir título do gráfico
        chart.set_title({
            'name': f'{title_chart} \nRepresentatividade de tipos de ocorrência\n{sum_first_data} manifestações - {date_start_format}',
            'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'},
            'overlay': True})
        chart.set_legend({
            'position': 'left',  # Define a posição da legenda
            'font': {'name': 'Segoe UI', 'size': 14, 'color': 'black'}})
        chart_sheet.set_chart(chart)

    @classmethod
    def chart_pie_occurrences_final_period(cls, workbook, sheet_name, df, points, title_chart, date_end_format, sum_final_data):
        chart_sheet = workbook.add_chartsheet(f'Graf_Ocorrencia {date_end_format}')
        chart_sheet.set_tab_color('#32CD32')
        chart = workbook.add_chart({'type': 'pie'})
        chart.add_series({
            'name': 'Ocorrências',
            'categories': [sheet_name, 1, 0, df.shape[0], 0],
            'values': [sheet_name, 1, 1, df.shape[0], 1],
            'data_labels': {'percentage': True, 'position': 'outside_end',
                            'font': {'name': 'Segoe UI', 'size': 12, 'color': 'black'}},
            'points': points})
        chart.set_title({
            'name': f'{title_chart} \nRepresentatividade de tipos de ocorrência\n{sum_final_data} manifestações - {date_end_format}',
            'name_font': {'name': 'Segoe UI', 'size': 16, 'bold': False, 'color': 'black'},
            'overlay': True
        })
        chart.set_legend({
            'position': 'left',  # Define a posição da legenda
            'font': {'name': 'Segoe UI', 'size': 14, 'color': 'black'}})
        chart_sheet.set_chart(chart)