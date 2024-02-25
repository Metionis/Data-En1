from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'cookiescooker',
    'start_date': datetime(2024, 2, 25, 11, 44)
}


# Getting API from a website that generate random api for this
def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent=3))

    return res


# Format the data to make it more easy to use
def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data



def stream_data():
    import json

    # Calling get_data function
    res = get_data()

    # After got the data use foramt_data function to format it
    res = format_data(res)

    print(json.dumps(res, indent=3))



with DAG('user_automation',
        default_args=default_args,
        schedule='@daily',
        catchup=False 
    ) as dag:

    streaming_task = PythonOperator (
        task_id='streaming_data_from_api',
        python_callable=stream_data
    )


stream_data();