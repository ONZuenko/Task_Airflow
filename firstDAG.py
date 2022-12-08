from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import random
#import psycopg2

#pg_hostname='host.docker.internal'
#pg_port='5430'
#pg_username='postgres'
#ps_pass='password'
#pg_db='test'

#def connect_to_psql()
#    conn=psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=ps_pass, database=pg_db)
#    cursor=conn.cursor()
#    cursor.close()
#    conn.close()

def hello():
    print("Airflow")

def numbers():
    n1=str(random.random())
    n2=str(random.random())
    with open("D:\\file.txt", "a", encoding='utf-8') as file:
        file.write(n1+" "+n2+"\n")
        file.close()

def c_numbers():
    with open("D:\\file.txt", "r+", encoding='utf-8') as file:
        l = file.readlines()
        l=l[:-1]
        for i, j in l:
            i = i.split(' ', 0)
            j = i.split(' ', 1)
        s1=sum(i, 1)
        s2=sum(j, 1)    
        s3=str(s1-s2)
        file.write(s3)    
        file.close()
        
with DAG(
    dag_id="first_dag", 
    start_date=datetime(2022, 1, 1), 
    schedule_interval="54-59/1 * * * *"
) as dag:
        bash_task=BashOperator(task_id="hello", bash_command="echo hello")
        python_task=PythonOperator(task_id="world", python_callable=hello)
        python_task1=PythonOperator(task_id="a_numbers", python_callable=numbers)
        python_task2=PythonOperator(task_id="c_numbers", python_callable=c_numbers)
        bash_task>>python_task>>python_task1>>python_task2