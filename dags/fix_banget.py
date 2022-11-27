# from __future__ import annotations
from webbrowser import get

import mysql.connector

import logging
import os
import shutil
import sys
import tempfile
import time
import requests
import pendulum
import pandas as pd
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.subdag import SubDagOperator

from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import task
import sqlalchemy
import mysql.connector
from airflow.utils.task_group import TaskGroup
from sqlalchemy.exc import SQLAlchemyError

HOST_MYSQL = '172.30.128.1'
USER_MYSQL = 'root'
PASSWORD_MYSQL = 'password'
DATABASE_MYSQL = 'data_enginner'
PORT_SQL = '3310'



HOST_MYSQL_2 = '172.30.128.1'
USER_MYSQL_2 = 'root'
PASSWORD_MYSQL_2 = 'password'
DATABASE_MYSQL_2 = 'data_enginner_master'
PORT_SQL_2 = '3308'



HOST_POSTGRES = '10.233.70.131'
USER_POSTGRES = 'postgres'
PASSWORD_POSTGRES = '1234'
DATABASE_POSTGRES = 'postgres'
PORT_POSTGRES = '5433'

ENGINE_POSTGES = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER_POSTGRES, PASSWORD_POSTGRES, HOST_POSTGRES, DATABASE_POSTGRES))



def print_context(ds=None, **kwargs):
    hit = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian?level=kab').json()
    getContent = hit['data']['content']
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print('iniadalah' + dir_path)
    df = pd.json_normalize(getContent)
    df['tanggal'] = pd.to_datetime(df['tanggal'], format='%Y-%m-%d')
    kwargs['ti'].xcom_push(key='value from pusher 1', value=getContent)
    return df.to_csv('./test.csv', header=True, index=False, quoting=1)


def getData(ds=None, **kwargs):
    ti = kwargs['ti']
    responseAPI = ti.xcom_pull(key=None, task_ids='push')
    df = pd.json_normalize(responseAPI)
    try:
        dropTable = "DROP table IF EXISTS staging_de"
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL, PASSWORD_MYSQL, HOST_MYSQL,PORT_SQL, DATABASE_MYSQL))
        ENGINE_SQL.execute(dropTable)
        getDF = df.to_sql(con=ENGINE_SQL, name='staging_de', index=False)
        df = pd.json_normalize(getDF)
    except Exception as e:
        print(e)



def insertProvince(ds=None, **kwargs):
    try:
        cn = mysql.connector.connect(host=HOST_MYSQL,user = USER_MYSQL,password= PASSWORD_MYSQL,database = DATABASE_MYSQL,port = PORT_SQL)
        myCursor = cn.cursor()
        query = f"select kode_prov,nama_prov from data_enginner.staging_de group by kode_prov, nama_prov"
        myCursor.execute(query)
        hasil = myCursor.fetchall()
        df = pd.DataFrame(hasil)
        df.columns = ['province_id','province_name']
        dropTable = "DROP table IF EXISTS Province_Table"
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL, PASSWORD_MYSQL, HOST_MYSQL,PORT_SQL, DATABASE_MYSQL))
        ENGINE_SQL.execute(dropTable)
        df.to_sql(con=ENGINE_SQL, name='Province_Table', index=False)


        return hasil
    except Exception as e:
        return e

def insertDistrict(ds=None, **kwargs):
    try:
        cn = mysql.connector.connect(host=HOST_MYSQL,user = USER_MYSQL,password= PASSWORD_MYSQL,database = DATABASE_MYSQL,port = PORT_SQL)
        myCursor = cn.cursor()
        query = f"select kode_kab,kode_prov,nama_kab from data_enginner.staging_de group by kode_kab, kode_prov, nama_kab"
        myCursor.execute(query)
        hasil = myCursor.fetchall()
        df = pd.DataFrame(hasil)
        df.columns = ['kode_kab','kode_prov','nama_kab']
        dropTable = "DROP table IF EXISTS district_table"
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL, PASSWORD_MYSQL, HOST_MYSQL,PORT_SQL, DATABASE_MYSQL))
        ENGINE_SQL.execute(dropTable)
        df.to_sql(con=ENGINE_SQL, name='district_table', index=False)
        return hasil
    except Exception as e:
        return e


def insertToPostgres(ds = None,**kwargs):
    try:
        p = "CREATE TABLE ILHAM_GANTENG"
        ENGINE_POSTGES.execute(p)
    except SQLAlchemyError as e:
        print(e)


def insertProvinceMonthlyToPostgree(ds=None, **kwargs):
    try:
        cn = mysql.connector.connect(host=HOST_MYSQL,user = USER_MYSQL,password= PASSWORD_MYSQL,database = DATABASE_MYSQL,port = PORT_SQL)
        myCursor = cn.cursor(dictionary=True)
        query = f"select year(tanggal) as Tahun,month(tanggal) as bulan,kode_prov,nama_prov,sum(SUSPECT),sum(CLOSECONTACT),sum(PROBABLE),sum(suspect_diisolasi),sum(suspect_discarded),sum(closecontact_dikarantina),sum(closecontact_discarded),sum(probable_diisolasi),sum(probable_discarded),sum(CONFIRMATION),sum(confirmation_sembuh),sum(confirmation_meninggal),sum(suspect_meninggal),sum(closecontact_meninggal),sum(probable_meninggal) " \
                f"from staging_de " \
                f"group by year(tanggal),month(tanggal), kode_prov, nama_prov order by year(tanggal),month(tanggal)"
        myCursor.execute(query)
        hasil = myCursor.fetchall()
        print(hasil)
        df = pd.DataFrame(hasil)
        df.columns = ['Tahun','Bulan','kode_prov','nama_prov','SUSPECT','CLOSECONTACT','PROBABLE','suspect_diisolasi','suspect_discarded','closecontact_dikarantina','closecontact_discarded','probable_diisolasi','probable_discarded','CONFIRMATION','confirmation_sembuh','confirmation_meninggal','suspect_meninggal','closecontact_meninggal','probable_meninggal']
        print(df)
        dropTable = "DROP table IF EXISTS monthly_fact"
        # ENGINE_SQL = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USER_POSTGRES, PASSWORD_POSTGRES, HOST_POSTGRES,PORT_POSTGRES, DATABASE_POSTGRES))
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL_2, PASSWORD_MYSQL_2, HOST_MYSQL_2,PORT_SQL_2, DATABASE_MYSQL_2))
        # ENGINE_POSTGES = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER_POSTGRE, PASSWORD_POSTGRE, HOST_POSTGRE, DATABASE_POSTGRE))
        ENGINE_SQL.execute(dropTable)
        df.to_sql(con=ENGINE_SQL, name='monthly_fact', index=False)
    except Exception as e:
        return e

def insertDistrictMonthlyToPostgree(ds=None, **kwargs):
    try:
        cn = mysql.connector.connect(host=HOST_MYSQL,user = USER_MYSQL,password= PASSWORD_MYSQL,database = DATABASE_MYSQL,port = PORT_SQL)
        myCursor = cn.cursor(dictionary=True)
        query = f"select year(tanggal) as Tahun,month(tanggal) as bulan,kode_kab,nama_kab,sum(SUSPECT),sum(CLOSECONTACT),sum(PROBABLE),sum(suspect_diisolasi),sum(suspect_discarded),sum(closecontact_dikarantina),sum(closecontact_discarded),sum(probable_diisolasi),sum(probable_discarded),sum(CONFIRMATION),sum(confirmation_sembuh),sum(confirmation_meninggal),sum(suspect_meninggal),sum(closecontact_meninggal),sum(probable_meninggal) " \
                f"from staging_de " \
                f"group by year(tanggal),month(tanggal), kode_kab, nama_kab order by year(tanggal),month(tanggal)"
        myCursor.execute(query)
        hasil = myCursor.fetchall()
        print(hasil)
        df = pd.DataFrame(hasil)
        df.columns = ['Tahun','Bulan','kode_prov','nama_prov','SUSPECT','CLOSECONTACT','PROBABLE','suspect_diisolasi','suspect_discarded','closecontact_dikarantina','closecontact_discarded','probable_diisolasi','probable_discarded','CONFIRMATION','confirmation_sembuh','confirmation_meninggal','suspect_meninggal','closecontact_meninggal','probable_meninggal']
        print(df)
        dropTable = "DROP table IF EXISTS monthly_fact_district"
        # ENGINE_SQL = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USER_POSTGRES, PASSWORD_POSTGRES, HOST_POSTGRES,PORT_POSTGRES, DATABASE_POSTGRES))
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL_2, PASSWORD_MYSQL_2, HOST_MYSQL_2,PORT_SQL_2, DATABASE_MYSQL_2))
        # ENGINE_POSTGES = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER_POSTGRE, PASSWORD_POSTGRE, HOST_POSTGRE, DATABASE_POSTGRE))
        ENGINE_SQL.execute(dropTable)
        df.to_sql(con=ENGINE_SQL, name='monthly_fact_district', index=False)
    except Exception as e:
        return e


def insertDistrictYearlyToPostgree(ds=None, **kwargs):
    try:
        cn = mysql.connector.connect(host=HOST_MYSQL,user = USER_MYSQL,password= PASSWORD_MYSQL,database = DATABASE_MYSQL,port = PORT_SQL)
        myCursor = cn.cursor(dictionary=True)
        query = f"select year(tanggal) as Tahun,kode_kab,nama_kab,sum(SUSPECT),sum(CLOSECONTACT),sum(PROBABLE),sum(suspect_diisolasi),sum(suspect_discarded),sum(closecontact_dikarantina),sum(closecontact_discarded),sum(probable_diisolasi),sum(probable_discarded),sum(CONFIRMATION),sum(confirmation_sembuh),sum(confirmation_meninggal),sum(suspect_meninggal),sum(closecontact_meninggal),sum(probable_meninggal) " \
                f"from staging_de " \
                f"group by year(tanggal), kode_kab, nama_kab order by year(tanggal)"
        myCursor.execute(query)
        hasil = myCursor.fetchall()
        print(hasil)
        df = pd.DataFrame(hasil)
        df.columns = ['Tahun','kode_prov','nama_prov','SUSPECT','CLOSECONTACT','PROBABLE','suspect_diisolasi','suspect_discarded','closecontact_dikarantina','closecontact_discarded','probable_diisolasi','probable_discarded','CONFIRMATION','confirmation_sembuh','confirmation_meninggal','suspect_meninggal','closecontact_meninggal','probable_meninggal']
        print(df)
        dropTable = "DROP table IF EXISTS yearly_fact_district"
        # ENGINE_SQL = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USER_POSTGRES, PASSWORD_POSTGRES, HOST_POSTGRES,PORT_POSTGRES, DATABASE_POSTGRES))
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL_2, PASSWORD_MYSQL_2, HOST_MYSQL_2,PORT_SQL_2, DATABASE_MYSQL_2))
        # ENGINE_POSTGES = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER_POSTGRE, PASSWORD_POSTGRE, HOST_POSTGRE, DATABASE_POSTGRE))
        ENGINE_SQL.execute(dropTable)
        df.to_sql(con=ENGINE_SQL, name='yearly_fact_district', index=False)
    except Exception as e:
        return e


def insertProvinceDailyToPostgree(ds=None, **kwargs):
    try:
        cn = mysql.connector.connect(host=HOST_MYSQL,user = USER_MYSQL,password= PASSWORD_MYSQL,database = DATABASE_MYSQL,port = PORT_SQL)
        myCursor = cn.cursor(dictionary=True)
        query = f"select tanggal ,kode_prov,nama_prov,sum(SUSPECT),sum(CLOSECONTACT),sum(PROBABLE),sum(suspect_diisolasi),sum(suspect_discarded),sum(closecontact_dikarantina),sum(closecontact_discarded),sum(probable_diisolasi),sum(probable_discarded),sum(CONFIRMATION),sum(confirmation_sembuh),sum(confirmation_meninggal),sum(suspect_meninggal),sum(closecontact_meninggal),sum(probable_meninggal) " \
                f"from staging_de " \
                f"group by tanggal, kode_prov, nama_prov order by year(tanggal),month(tanggal)"
        myCursor.execute(query)
        hasil = myCursor.fetchall()
        print(hasil)
        df = pd.DataFrame(hasil)
        df.columns = ['Tanggal','kode_prov','nama_prov','SUSPECT','CLOSECONTACT','PROBABLE','suspect_diisolasi','suspect_discarded','closecontact_dikarantina','closecontact_discarded','probable_diisolasi','probable_discarded','CONFIRMATION','confirmation_sembuh','confirmation_meninggal','suspect_meninggal','closecontact_meninggal','probable_meninggal']
        print(df)
        dropTable = "DROP table IF EXISTS daily_fact"
        # ENGINE_SQL = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USER_POSTGRES, PASSWORD_POSTGRES, HOST_POSTGRES,PORT_POSTGRES, DATABASE_POSTGRES))
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL_2, PASSWORD_MYSQL_2, HOST_MYSQL_2,PORT_SQL_2, DATABASE_MYSQL_2))
        # ENGINE_POSTGES = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER_POSTGRE, PASSWORD_POSTGRE, HOST_POSTGRE, DATABASE_POSTGRE))
        ENGINE_SQL.execute(dropTable)
        df.to_sql(con=ENGINE_SQL, name='daily_fact', index=False)
    except Exception as e:
        return e

def insertProvinceYearlyToPostgree(ds=None, **kwargs):
    try:
        cn = mysql.connector.connect(host=HOST_MYSQL,user = USER_MYSQL,password= PASSWORD_MYSQL,database = DATABASE_MYSQL,port = PORT_SQL)
        myCursor = cn.cursor(dictionary=True)
        query = f"select year(tanggal) as Tahun,kode_prov,nama_prov,sum(SUSPECT),sum(CLOSECONTACT),sum(PROBABLE),sum(suspect_diisolasi),sum(suspect_discarded),sum(closecontact_dikarantina),sum(closecontact_discarded),sum(probable_diisolasi),sum(probable_discarded),sum(CONFIRMATION),sum(confirmation_sembuh),sum(confirmation_meninggal),sum(suspect_meninggal),sum(closecontact_meninggal),sum(probable_meninggal) " \
                f"from staging_de " \
                f"group by year(tanggal), kode_prov, nama_prov order by year(tanggal)"
        myCursor.execute(query)
        hasil = myCursor.fetchall()
        print(hasil)
        df = pd.DataFrame(hasil)
        df.columns = ['Tahun','kode_prov','nama_prov','SUSPECT','CLOSECONTACT','PROBABLE','suspect_diisolasi','suspect_discarded','closecontact_dikarantina','closecontact_discarded','probable_diisolasi','probable_discarded','CONFIRMATION','confirmation_sembuh','confirmation_meninggal','suspect_meninggal','closecontact_meninggal','probable_meninggal']
        print(df)
        dropTable = "DROP table IF EXISTS yearly_fact"
        # ENGINE_SQL = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USER_POSTGRES, PASSWORD_POSTGRES, HOST_POSTGRES,PORT_POSTGRES, DATABASE_POSTGRES))
        ENGINE_SQL = sqlalchemy.create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USER_MYSQL_2, PASSWORD_MYSQL_2, HOST_MYSQL_2,PORT_SQL_2, DATABASE_MYSQL_2))
        # ENGINE_POSTGES = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER_POSTGRE, PASSWORD_POSTGRE, HOST_POSTGRE, DATABASE_POSTGRE))
        ENGINE_SQL.execute(dropTable)
        df.to_sql(con=ENGINE_SQL, name='yearly_fact', index=False)
    except Exception as e:
        return e

def dump(ds = None,**kwargs):
    print('hello')

with DAG(
        dag_id='Tugas_DE',
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=['example']
) as dag:

    hitAPI = PythonOperator(
        task_id= 'push',
        python_callable=print_context
    )
    getData = PythonOperator(
        task_id='loadData',
        python_callable=getData
    )





    with TaskGroup('Pengambilan_Data_Staging_Dari_API',tooltip='Get_Data_From_STG') as getDataStaging:
        getStaging = PythonOperator(
            task_id ='get_Data_From_Staging',
            python_callable=insertProvince
        )
        getStagingDistrict = PythonOperator(
            task_id ='get_Data_From_Staging_District',
            python_callable=insertDistrict
        )
        getStaging >> [getStagingDistrict]

    with TaskGroup('Pengeolahaan_Landing_to_Master',tooltip='Get_Data_From_STG') as mysqlToPostgree:
        provinceToPostgreeDaily = PythonOperator(
            task_id= 'get_staging_to_daily',
            python_callable= insertProvinceDailyToPostgree
        )
        provinceToPostgree = PythonOperator(
            task_id= 'get_staging_to_monthly',
            python_callable= insertProvinceMonthlyToPostgree
        )

        provinceToPostgreeYearly = PythonOperator(
            task_id= 'get_staging_to_yearly',
            python_callable= insertProvinceYearlyToPostgree
        )

        districtToPostgreeMonthly = PythonOperator(
            task_id= 'get_staging_to_monthly_district',
            python_callable= insertDistrictMonthlyToPostgree
        )

        districtToPostgreeYearly = PythonOperator(
            task_id= 'get_staging_to_yearly_district',
            python_callable= insertDistrictYearlyToPostgree
        )

        getStagingDistrict >> [provinceToPostgreeDaily,provinceToPostgree,provinceToPostgreeYearly,districtToPostgreeMonthly,districtToPostgreeYearly]



    # @task(task_id="print_the_context")
    # def print_context(ds=None, **kwargs):
    #     hit = requests.get('https://covid19-public.digitalservice.id/api/v1/rekapitulasi_v2/jabar/harian').json()
    #     getContent = hit['data']['content']
    #     # jsonText = json.dumps(hit,indent=4)
    #     df = pd.json_normalize(getContent)
    #     df['tanggal'] = pd.to_datetime(df['tanggal'], format='%Y-%m-%d')
    #     kwargs['ti'].xcom_push(key='value from pusher 1', value=df)
    # run_this = print_context()

    # @task(task_id="getDF")
    # def getDataFrame(ds = None,**kwargs):
    #     ti = kwargs['ti']
    #     pulled_value_1 = ti.xcom_pull(key=None, task_ids='print_context')


    hitAPI >> getData >> getStaging