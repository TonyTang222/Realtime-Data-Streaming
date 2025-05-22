import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Tony',
    'start_date':datetime(2025,5,20,00,00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']} {location['postcode']}, {location['state']}, {location['country']}"
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('user_data',json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'Error occured: {e}')
            continue
    #print(json.dumps(res,indent=3))

with DAG('user_auto_loading',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False) as dag:

    streaming_data = PythonOperator(
          task_id='streaming_data_from_api',
          python_callable=stream_data
)