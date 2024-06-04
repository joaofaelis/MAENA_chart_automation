# Standard
import locale
from datetime import datetime
# Local
from src.repository.SQL.repository import SQLRepository
from src.models.style_dfs.style import StylesDfs
# Third Party
import pandas as pd

class VariableTreatment:

    style = StylesDfs()
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
    @classmethod
    def date_formatting_initial(cls, start_date):
        start_date_in = datetime.strptime(start_date, "%Y-%m")
        start_date_formatting = start_date_in.strftime("%b/%Y").capitalize()
        return start_date_formatting
    @classmethod
    def date_formatting_end(cls, end_date):
        end_date_in = datetime.strptime(end_date, "%Y-%m")
        end_date_formatting = end_date_in.strftime("%b/%Y").capitalize()
        return end_date_formatting

    @classmethod
    def chart_title_escopo(cls, num_escopo):
        get_escopo = SQLRepository.get_escopo_name_bd(num_escopo)
        escopo_name = get_escopo['Nome_Escopo_Hierarquia'].to_string(index=False)
        chart_title = escopo_name.replace('[', '').replace(']', '')
        return chart_title

    @classmethod
    def date_order_channel(cls, escopo_trad, escopo_dig, start_date, end_date):
        df_evolucao_p_canal = SQLRepository.evolução_por_canal(escopo_trad, escopo_dig, start_date, end_date)
        df_evolucao_p_canal['Mes_ano'] = pd.to_datetime(df_evolucao_p_canal['Mes_ano'], format='%Y-%m')
        df_evolucao_p_canal['Mes_ano'] = df_evolucao_p_canal['Mes_ano'].dt.strftime('%b/%y').str.capitalize()
        return df_evolucao_p_canal

    @classmethod
    def order_occurrences_chart_pie(cls, escopo, start_date, end_date):
        df_pie_timestamp = SQLRepository.repren_tipos_ocorrencia_timestamp_period(escopo, start_date, end_date)
        df_pie_timestamp['Order'] = df_pie_timestamp['Tipo_de_ocorrencia'].map(cls.style.order_color_chart_ocorrencias)
        df_pie_timestamp_order = df_pie_timestamp.sort_values(by='Order')
        df_pie_timestamp_order['Tipo_de_ocorrencia'] = df_pie_timestamp_order['Tipo_de_ocorrencia'].str.capitalize()
        df_pie_timestamp_order['Tipo_de_ocorrencia'] = df_pie_timestamp_order['Tipo_de_ocorrencia'].str.title()
        df_pie_timestamp_order_lock = df_pie_timestamp_order.drop(columns=['Order'])
        return df_pie_timestamp_order_lock
    @classmethod
    def order_chart_pie_first_period(cls, escopo, start_date):
        df_pie_first_period = SQLRepository.repren_tipos_ocorrencia_first_period(escopo, start_date)
        df_pie_first_period['Order'] = df_pie_first_period['Tipo_de_ocorrencia'].map(cls.style.order_color_chart_ocorrencias)
        df_pie_order_first_period = df_pie_first_period.sort_values(by='Order')
        df_pie_order_first_period['Tipo_de_ocorrencia'] = df_pie_order_first_period['Tipo_de_ocorrencia'].str.capitalize()
        df_pie_order_first_period['Tipo_de_ocorrencia'] = df_pie_order_first_period['Tipo_de_ocorrencia'].str.title()
        df_pie_timestamp_first_lock = df_pie_order_first_period.drop(columns=['Order'])
        return df_pie_timestamp_first_lock

    @classmethod
    def order_chart_pie_final_period(cls, escopo, end_date):
        df_pie_final_period = SQLRepository.repren_tipos_ocorrencia_final_period(escopo, end_date)
        df_pie_final_period['Order'] = df_pie_final_period['Tipo_de_ocorrencia'].map(cls.style.order_color_chart_ocorrencias)
        df_pie_order_final = df_pie_final_period.sort_values(by='Order')
        df_pie_order_final['Tipo_de_ocorrencia'] = df_pie_order_final['Tipo_de_ocorrencia'].str.capitalize()
        df_pie_order_final['Tipo_de_ocorrencia'] = df_pie_order_final['Tipo_de_ocorrencia'].str.title()
        df_pie_timestamp_final_lock = df_pie_order_final.drop(columns=['Order'])
        return df_pie_timestamp_final_lock

    @classmethod
    def get_sum_total(cls, escopo, start_date, end_date):
        total_manifestation = SQLRepository.get_total_manifestation_bd(escopo, start_date, end_date)
        soma_total_geral = int(total_manifestation['Total'].sum())
        return soma_total_geral
    @classmethod
    def get_sum_timestamp_chart_pie(cls, escopo, start_date, end_date):
        df_pie_timestamp = SQLRepository.repren_tipos_ocorrencia_timestamp_period(escopo, start_date, end_date)
        sum_period_pie_data = int(df_pie_timestamp['Total'].sum())
        return sum_period_pie_data

    @classmethod
    def get_sum_first_period_chart_pie(cls, escopo, start_date):
        df_pie_first_period = SQLRepository.repren_tipos_ocorrencia_first_period(escopo, start_date)
        sum_first_data = int(df_pie_first_period['Total'].sum())
        return sum_first_data

    @classmethod
    def get_sum_end_period_chart_pie(cls, escopo, end_date):
        df_pie_final_period = SQLRepository.repren_tipos_ocorrencia_final_period(escopo, end_date)
        sum_final_data = int(df_pie_final_period['Total'].sum())
        return sum_final_data