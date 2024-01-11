'''
=================================================
Milestone 3

Nama  : Muhammad Insani
Batch : FTDS-010-HCK

Program ini diciptakan untuk otomatisasi ETL dari dataset 'data house production' di PostgreSQL ke ElasticSearch 
dan visualisasinya melalui dashboard Kibana.
=================================================
'''

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def fetch_from_postgresql():
    '''
    
    Fungsi ini ditujukan mengambil data dari PostgreSQL dan menyimpannya sebagai file CSV.
    
    '''
    # Koneksi ke PostgreSQL
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()
    
    # Membaca data dari tabel PostgreSQL
    df=pd.read_sql_query("select * from table_m3", conn)
    
    # Menyimpan data ke file CSV
    df.to_csv('/opt/airflow/data/P2M3_muhammad_insani_data_raw.csv', index=False)
    
def data_cleaning():
    '''
    
    Membersihkan data dari nilai yang hilang dan merapikan struktur kolom.

    '''
    
    # Membaca data dari file CSV yang telah diambil sebelumnya
    df=pd.read_csv("/opt/airflow/data/P2M3_muhammad_insani_data_raw.csv")
    
    # Menghapus baris yang duplikat
    df.drop_duplicates(inplace=True)

    # Mengonversi kolom-kolom tertentu ke tipe data integer
    df['PTS Existing Units']=df['PTS Existing Units'].astype(int)
    df['PTS Proposed Units']=df['PTS Proposed Units'].astype(int)
    df['Proposed Units']=df['Proposed Units'].astype(int)
    df['Net Units']=df['Net Units'].astype(int)
    df['Net Units Completed']=df['Net Units Completed'].astype(int)

    # Merapikan nama kolom
    df.columns = [x.lower().replace(' ', '_') for x in df.columns]
    df.columns = ["".join(c if c.isalnum() or c == '_' else '' for c in x) for x in df.columns]

    # Mengisi nilai yang hilang
    df['affordable_units_estimate'].fillna(False, inplace=True) #karena nilai null nya itu sebagai tidak adanya unit yang terjangkau
    df['plan_area'].fillna('Unknown', inplace=True) #karena ingin mempertahankan konsistensi, interpretasi, dan kemudahan analisis dalam data.
    df['project_affordability_type'].fillna('Unknown', inplace=True) #karena ingin mempertahankan konsistensi, interpretasi, dan kemudahan analisis dalam data.
    df['ppts_project_id'].fillna('Unknown', inplace=True) #karena ingin mempertahankan konsistensi, interpretasi, dan kemudahan analisis dalam data.
    df.dropna(inplace=True) #karena nilai yang hilang relatif kecil agar menghindari bias
    
    # Menyimpan data yang telah dibersihkan
    df.to_csv('/opt/airflow/data/P2M3_muhammad_insani_data_clean.csv', index=False)


def post_to_elasticsearch():
    '''
    
    Mengirimkan data yang telah dibersihkan ke Elasticsearch.

    '''
    
    # Koneksi ke Elasticsearch
    es = Elasticsearch('http://elasticsearch:9200')
    
    # Membaca data yang telah dibersihkan 
    df = pd.read_csv('/opt/airflow/data/P2M3_muhammad_insani_data_clean.csv')
    
    # Mengirim setiap baris data sebagai dokumen ke Elasticsearch
    for i,r in df.iterrows():
        doc = r.to_json()
        res = es.index(index='milestone3_insan', body= doc)

# DAG Configuration
default_args = {
    'owner': 'insani',
    'start_date': dt.datetime(2023, 12, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes = 15),
}


with DAG('milestone3_insani',
         default_args = default_args,
         schedule_interval = "30 23 * * *",  # dieksekusi setiap hari pada pukul 23:30 UTC+0 setara dengan ( 06:30 WIB UTC+7)
         catchup = False) as dag:

    # Task untuk mengambil dan menyimpan data
    fetchAndSave = PythonOperator(task_id = 'fetch_save',
                                  python_callable = fetch_from_postgresql)
    
    # Task untuk membersihkan data
    cleanData = PythonOperator(task_id = 'clean',
                               python_callable = data_cleaning)
    
    # Task untuk mengirim data ke Elasticsearch
    insertToElastic = PythonOperator(task_id = 'insert_elastic',
                                     python_callable = post_to_elasticsearch)

# Menentukan urutan eksekusi task
fetchAndSave >> cleanData >> insertToElastic
