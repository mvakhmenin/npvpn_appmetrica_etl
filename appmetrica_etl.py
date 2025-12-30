import pandas as pd

from connectors.ch_connector import ClickHouseConnector
from connectors.appmetrica_connector import AppMetricaConnector
import logging
import os
from utils import get_logger
from config import appmetrica_endpoints
try:
    from dotenv import load_dotenv
    load_dotenv()
    print('!!! DEV ENVIRONMENT !!!')
except: pass

logger = get_logger(f"AppMetricaETL", logging.INFO)

CH_HOST = os.getenv('CH_HOST')
CH_PORT = os.getenv('CH_PORT')
CH_USER = os.getenv('CH_USER')
CH_PASS = os.getenv('CH_PASS')
CH_CERT = os.getenv('CH_CERT')
AM_TOKEN = os.getenv('AM_TOKEN')

client_ch = ClickHouseConnector(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASS,
    secure=True,
    verify=False,
    ca_cert=CH_CERT,
    log_level = logging.INFO
)
client_am = AppMetricaConnector(
    auth_token=AM_TOKEN,
    log_level = logging.INFO
)

def check_connections():
    connection_errors = []

    if client_ch.connect():
        logger.info('Успешное подключение к Clikchouse')
    else:
        logger.error('Ошибка при подключении к Clikchouse')
        connection_errors.append('Clikchouse')

    if client_am.test_connection():
        logger.info('Успешное подключение к App Metrica')
    else:
        logger.error('Ошибка при подключении к App Metrica')
        connection_errors.append('App Metrica')

    if connection_errors:
        client_ch.disconnect()
        raise ConnectionError(f'Ошибка при подключении к {" и ".join(connection_errors)}')
    return True

def do_app_etl(app_id: int):
    source_names = list(appmetrica_endpoints.keys())
    for source_name in source_names:
        logger.info(f'Запускаю получение данных для источника {source_name}')
        do_source_etl(app_id, source_name)
        logger.info(f'Успешное получение данных для источника {source_name}')

def do_source_etl(app_id: int, source_name: str):
    src_max_date = client_ch.get_target_max_date(source_name)
    src_data = client_am.get_source_data(source_name, app_id, src_max_date)
    df_src_data = pd.DataFrame(src_data)
    client_ch.insert_source_data(source_name, df_src_data)

def main():
    if check_connections():
        logger.info('Запускаю получение данных из App Metrica')
        app_id = 4804657
        logger.info('Запускаю получение данных для приложения NoProblem VPN')
        do_app_etl(app_id)
    client_ch.disconnect()

if __name__ == "__main__":
    main()