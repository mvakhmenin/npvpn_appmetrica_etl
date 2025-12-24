from connectors.ch_connector import ClickHouseConnector
import logging
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
    print('!!! DEV ENVIRONMENT !!!')
except: pass

CH_HOST = os.getenv('CH_HOST')
CH_PORT = os.getenv('CH_PORT')
CH_USER = os.getenv('CH_USER')
CH_PASS = os.getenv('CH_PASS')
CH_CERT = os.getenv('CH_CERT')
AM_TOKEN = os.getenv('AM_TOKEN')

client = ClickHouseConnector(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASS,
            secure=True,
            verify=False,
            ca_cert=CH_CERT,
            log_level = logging.INFO
)

result = client.connect()

if result: print('Подключился!!!')

print(client.execute_query('SELECT version()', False))

client.disconnect()