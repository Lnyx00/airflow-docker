## Final Presntation Data Enginner Digital Skola

This final project is about Dockerize ETL Pipeline using ETL tools Airflow that extract
Public API data from PIKOBAR, then load into MySQL (Staging Area) and finally
aggregate the data and save into PostgreSQL.


- Create Docker (MySQL, Airflow and PostgreSQL) in your local computer
  - download yaml from airflow 
  - https://airflow.apache.org/docs/apache-airflow/2.4.3/docker-compose.yaml
  - and put that file inside your folder
  - open curent folder with powershell and run this script
  - ```docker-compose up airflow-init```
  - wait this process until finish
  - then up the airflow service
  - ```docker compose up```
  - then airflow should be running right now
- Create Database in MySQL and PostgreSQL
  - > because the localhost ip can't be change im gonna use mysql to mysql
  - go to your folder and run this sciprt for download image mysql 
  - ```docker pull mysql```
  - after than run this script to make your sql running on docker
  - ```docker run --name mysql-[name_container] -p 3310:3306 -e MYSQL_ROOT_PASSWORD=“[your_password]" -d mysql:latest```
  - after that then mysql will be running on your docker

- Create Connection from Airflow to MySQL and PostgreSQL
  - create py file on dags (your current folder) connection on python
  - add this script
  - ```sh 
    HOST_MYSQL = '172.30.128.1'
    USER_MYSQL = 'root' 
    PASSWORD_MYSQL = 'password' 
    DATABASE_MYSQL = 'data_enginner' 
    PORT_SQL = '3310'
  - ```sh
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
- Create DAG
  - dag will automaticly generate dags
- Create DDL in MySQL 
  - run ddl script on dbeaver
  - check file mysql
- Get data from Public API covid19 and load data to MySQL 
  - ```sh
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
    
- Create DDL in PostgreSQL for Fact table and Dimension table 
- Create aggregate Province Daily save to Province Daily Table 
- Create aggregate Province Monthly save to Province Monthly Table 
- Create aggregate Province Yearly save to Province Yearly 
- Create aggregate District Monthly save to District Monthly 
- Create aggregate District Yearly save to District Yearly 
- Schedule the DAG daily save to Province Monthly



