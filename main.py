# pip install mysql-connector-python pandas
import mysql.connector
import pandas as pandas
import json

config = {}
with open('config.json') as config_file:
    config = json.load(config_file)

connnection = mysql.connector.connect(
  host=config['host'],
  user=config['user'],
  passwd=config['passwd'],
  database=config['database'],
)

name_exel = config['exel_file_name']
name_table = config['db_table_name']

dataframe = pandas.read_excel(name_exel)

sql_executor = connnection.cursor()
sql_executor.execute("BEGIN")

# sql_executor.execute('create table if not exists user_test(id int,active boolean,role varchar(255),name varchar(255),email varchar(255),password varchar(255))')

try:
    for index, row in dataframe.iterrows():
        data = {}
        for column in dataframe.columns:
            data[column] = row[column]
        insert_query = f"INSERT INTO {name_table} ({','.join(dataframe.columns)}) VALUES ({','.join(['%s']*len(dataframe.columns))})"
        values = tuple(data.values())
        sql_executor.execute(insert_query, values) 
    sql_executor.execute("COMMIT")
except Exception as e:
    print( "Error: ", e)
    sql_executor.execute("ROLLBACK")
    exit()
