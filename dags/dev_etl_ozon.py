from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.models import Variable

import requests
from datetime import datetime, timedelta
import time
import io
import pandas as pd


default_kwargs = {
    'owner': 'alexchevsky',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 28),
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'max_retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_exponential_backoff': True,
    'execution_timeout': timedelta(minutes=60),
    'spreadsheet_id': '1ApYbZ62vmuhM4D1vZ_SvLTZ4_Jx9rOxFBIKB6OJzhfw',
    'range': 'ads_sku!A1',
    'range_all': 'ads_sku!A1:Z',
    'date_from': (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
    'date_to': datetime.now().strftime("%Y-%m-%d"),
    'account_id': 'd75f3950-ff5d-4f4b-b39c-5d6c356fc91f',
    'bucket_name': 'chevsky-airflow-data'
}


def batchify(data, batch_size):
    """
    Splits data into batches of a specified size.

    Parameters:
    - data: The list of items to be batched.
    - batch_size: The size of each batch.

    Returns:
    - A generator yielding batches of data.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def get_creds(account_id, service):
    api_token = Variable.get("hq_token")
    url = 'http://ec2-54-93-221-26.eu-central-1.compute.amazonaws.com/api/credentials/'
    headers = {'Authorization': f'Token {api_token}'}
    params = {'account_id': account_id}
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    return [x['credentials'] for x in res.json() if x['service'] == service]


def authenticate(**kwargs):
    account_id = kwargs['account_id']
    creds = get_creds(account_id, 'ozon')

    url = 'https://performance.ozon.ru/api/client/token'

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    payload = {
        "client_id": creds[0]['client_id'],
        "client_secret": creds[0]['client_secret'],
        "grant_type": "client_credentials"
    }

    res = requests.post(url, headers=headers, json=payload)
    access_token = res.json()['access_token']

    ti = kwargs['ti']
    ti.xcom_push(key='access_token', value=access_token)


def get_campaigns(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='authenticate', key='access_token')

    url = 'https://performance.ozon.ru:443/api/client/campaign'

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    res = requests.get(url, headers=headers)
    ti.xcom_push(key='campaigns', value=res.json())


def get_sku_campaign_stats(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='authenticate', key='access_token')
    campaigns = ti.xcom_pull(task_ids='src__get_campaigns', key='campaigns')
    date_from = kwargs.get('date_from')
    date_to = kwargs.get('date_to')
    reports = []

    campaign_states = [
        'CAMPAIGN_STATE_RUNNING',
        'CAMPAIGN_STATE_STOPPED',
        'CAMPAIGN_STATE_INACTIVE',
        'CAMPAIGN_STATE_FINISHED'
    ]
    print('Campaigns:', len(campaigns['list']))
    target_campaigns = [c for c in campaigns['list'] if c.get('state') in campaign_states
                        and c.get('advObjectType') == 'SKU']
    target_campaign_ids = [c['id'] for c in target_campaigns]
    print(f'Target Campaign Ids ({len(target_campaign_ids)}):', target_campaign_ids)
    batches = list(batchify(target_campaign_ids, 10))
    print('Batches:', len(batches))

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    for i, b in enumerate(batches):
        print(f'Batch {i}/{len(batches)}')
        url = 'https://performance.ozon.ru:443/api/client/statistics/json'
        payload = {
            "campaigns": b,
            "dateFrom": date_from,
            "dateTo": date_to,
            "groupBy": "DATE"
        }

        res = requests.post(url, headers=headers, json=payload)

        res.raise_for_status()
        report_uuid = res.json()['UUID']

        report_ready = False

        while not report_ready:
            url = f'https://performance.ozon.ru:443/api/client/statistics/{report_uuid}'

            res = requests.get(url, headers=headers)
            if res.json()['state'] == 'OK':
                report_ready = True
            elif res.json()['state'] == 'ERROR':
                raise
            print('Report status:', res.status_code, res.json()['state'])
            time.sleep(5)

        url = 'https://performance.ozon.ru:443/api/client/statistics/report'
        payload = {
            'UUID': report_uuid
        }
        res = requests.get(url, headers=headers, params=payload)
        reports.append(res.json())

    full_data = []

    for r in reports:
        for campaign_id, data in r.items():
            camp = {
                'campaign_id': campaign_id,
                # 'campaign_title': data['title']
            }
            rows = [{**camp, **r} for r in data['report']['rows']]
            full_data += rows

    df = pd.DataFrame(full_data)

    float_columns = [
        'ctr',
        'avgBid',
        'moneySpent',
        'ordersMoney',
        'modelsMoney',
        'price'
    ]

    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
    df['updated_at'] = (
        pd
        .Timestamp
        .now(tz='UTC')
        .tz_convert('Europe/Moscow')
        .strftime('%Y-%m-%d %H:%M:%S')
        )

    for col in float_columns:
        df[col] = df[col].str.replace(',', '.').astype('float')

    col_names_mapping = {
        'campaign_id':  'РК ID',
        'date':         'День',
        'views':        'Показы',
        'clicks':       'Клики',
        'ctr':          'CTR (%)',
        'moneySpent':   'Расход, ₽, с НДС',
        'avgBid':       'Ср. цена клика, ₽',
        'orders':       'Заказы',
        'ordersMoney':  'Выручка, ₽',
        'models':       'Заказы модели',
        'modelsMoney':  'Выручка с заказов модели, ₽',
        'sku':          'sku',
        'title':        'Название товара',
        'price':        'Цена товара, ₽',
        'updated_at':   'Посл. обновление'
    }

    df = df[[
        'campaign_id',
        'date',
        'sku',
        'title',
        'price',
        'views',
        'clicks',
        'moneySpent',
        'orders',
        'ordersMoney',
        'models',
        'modelsMoney',
        'updated_at'
    ]].sort_values(['date', 'campaign_id'], ascending=[False, True])

    df.columns = [col_names_mapping[c] for c in df.columns]

    hook = S3Hook('aws')

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'sku_campaign_stats_{now_str}.csv'

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    hook = S3Hook('aws')
    bucket_name = kwargs.get('bucket_name')
    hook.load_string(
        string_data=buffer.getvalue(),
        bucket_name=bucket_name,
        key=file_name,
        replace=True
    )

    ti = kwargs['ti']
    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='file_name', value=file_name)


def upload_to_google_sheets(**kwargs):

    spreadsheet_id = kwargs.get('spreadsheet_id')
    range = kwargs.get('range')
    range_all = kwargs.get('range_all')

    ti = kwargs['ti']
    bucket_name = ti.xcom_pull(task_ids='src__get_sku_campaign_stats', key='bucket_name')
    file_name = ti.xcom_pull(task_ids='src__get_sku_campaign_stats', key='file_name')

    hook = S3Hook('aws')
    file_content = hook.read_key(key=file_name, bucket_name=bucket_name)
    df = pd.read_csv(io.StringIO(file_content))

    headers = df.columns.values.tolist()
    values = df.values.tolist()

    hook = GSheetsHook(gcp_conn_id="gc")
    hook.clear(spreadsheet_id, range_all)
    hook.update_values(spreadsheet_id, range, [headers] + values)


with DAG(
        'ozon_etl',
        default_args=default_kwargs,
        description='Getting Ozon API Data',
        start_date=datetime(2024, 6, 18),
        schedule_interval='0 2 * * *') as dag:

    task_authenticate = PythonOperator(
        task_id='authenticate',
        python_callable=authenticate,
        op_kwargs=default_kwargs,
    )

    task_get_campaigns = PythonOperator(
        task_id='src__get_campaigns',
        python_callable=get_campaigns,
        op_kwargs=default_kwargs,
    )

    task_get_sku_campaign_stats = PythonOperator(
        task_id='src__get_sku_campaign_stats',
        python_callable=get_sku_campaign_stats,
        op_kwargs=default_kwargs,
    )

    task_upload_to_google_sheets = PythonOperator(
        task_id='upload_to_google_sheets',
        python_callable=upload_to_google_sheets,
        op_kwargs=default_kwargs,
    )

    task_authenticate >> task_get_campaigns >> task_get_sku_campaign_stats
    task_get_sku_campaign_stats >> task_upload_to_google_sheets
