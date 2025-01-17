

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pendulum
from airflow.macros import ds_add
from os.path import join
import pandas as pd
import os

with DAG(
   "dados_climaticos",
   start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
   schedule_interval='0 0 * * 1', 
) as dag: 

    tarefa_1 = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p "/home/amandageovanna/Documents/airflowalura/semana={{ data_interval_end.strftime("%Y-%m-%d") }}"'
    )
    
    def extrai_dados(data_interval_end):
        city = 'boston'
        key = 'HVDBZL3L28UK7LKEQDZCYHZ66'

        data_interval_end_date = data_interval_end.split('T')[0]
        
        end_date = ds_add(data_interval_end_date, 7)

        URL = join(
            'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end_date}/{end_date}',
            f'?unitGroup=metric&include=days&key={key}&contentType=csv'
        )      

        dados = pd.read_csv(URL)
        
        file_path = f'/home/amandageovanna/Documents/airflowalura/semana={data_interval_end_date}/'
        os.makedirs(file_path, exist_ok=True)

        try:
            dados.to_csv(file_path + 'dados_brutos.csv')
            dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
            dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
        except Exception as e:
            print(f"Erro ao salvar os arquivos: {e}")
            raise

    tarefa_2 = PythonOperator(
        task_id='extrai_dados',
        python_callable=extrai_dados,
        op_kwargs={'data_interval_end': '{{ data_interval_end }}'}
    )

    tarefa_1 >> tarefa_2



