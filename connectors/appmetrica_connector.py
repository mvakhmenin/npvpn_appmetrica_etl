import requests
import logging
from datetime import datetime, timedelta
import time
from typing import Optional, Union, List, Dict

from utils import get_logger
from config import app_ids, appmetrica_endpoints, appmetrica_fields


class AppMetricaConnector:
    """Коннектор для App Metrica"""
    
    def __init__(self, 
                 auth_token: str,
                 log_level: int = logging.WARNING):
        self.auth_token = auth_token
        self.base_url = 'https://api.appmetrica.yandex.ru'

        self.logger = get_logger(f"AppMetricaConnector", log_level)

        with requests.Session() as self.session:
            adapter = requests.adapters.HTTPAdapter(max_retries=20)
            self.session.mount('https://', adapter)
            self.session.mount('http://', adapter)

        self.headers = {
            'Authorization': 'OAuth ' + self.auth_token
        }
        self.params = None

    def test_connection(self) -> bool:
        """Проверка соединения с App Metrica"""
        test_result = False
        test_endpoint = '/management/v1/applications'
        r = self.session.get(self.base_url + test_endpoint, headers=self.headers)
        if r.status_code == 200:
            test_result = True
            self.logger.info(f"Успешное подключение к App Metrica")
        else:
            self.logger.error(f"Ошибка при подключение к App Metrica: {r.text}")
        
        return test_result

    def get_source_data(self, 
                        source: str, 
                        app: str,
                        date_from: datetime):
        """
        Получение актуальных данных из App Metrica
        """
        result_data = None
        if self._request_source_data(source, app, date_from):
            result_data = self._get_data_from_source()
        return result_data['data']

    def _request_source_data(self, 
                            source: str, 
                            app: int,
                            date_from: datetime) -> bool:
        """
        Отправка запроса данных в App Metrica
        Запрашиваются данные с `date_from` до конца предыдущего дня
        
        :param self: Description
        :param source: Description
        :type source: str
        :param app: Description
        :type app: int
        :param date_from: Description
        :type date_from: datetime
        """
        
        date_from = date_from.strftime('%Y-%m-%d %H:%M:%S')

        date_until = datetime.now()
        date_until = date_until.replace(hour=0, minute=0, second=0, microsecond=0)
        date_until = date_until - timedelta(seconds=1)
        date_until = date_until.strftime('%Y-%m-%d %H:%M:%S')

        self.params = {
            'application_id': app,
            'date_since': date_from,
            'date_until': date_until,
            'fields':appmetrica_fields[source]
        }
        self.data_request_endpoint = appmetrica_endpoints[source]
        response = self.session.get(self.base_url + self.data_request_endpoint, 
                                    headers=self.headers, 
                                    params=self.params)
        if response.status_code == 202:
            self.logger.info(f"Успешная отправка запроса в App Metrica (приложение {app}, тип {source}, период с {date_from} по {date_until}): {response.text}")
            return True
        elif response.status_code == 200:
            self.logger.warning(f"Запрос с такими параметрами уже был отправлен ранее (приложение {app}, тип {source}, период с {date_from} по {date_until})")
            return True
        else:
            self.logger.error(f"Ошибка при отправке запроса: {response.status_code=}, {response.text}")
            return False
        
    def _get_data_from_source(self, 
                              n_retries: int = 40, 
                              wait_s: int = 30) -> Union[bool, List[Dict]]:
        """
        Ожидаение ответа App Metrica на запрос и возрват его результата
        
        :param self: Description
        :param n_retries: Description
        :type n_retries: int
        """
        if not self.params:
            self.logger.error(f"Ошибка при ожидании ответа: запрос не был отправлен")
            return False
        
        for i in range(n_retries):
            response = self.session.get(self.base_url + self.data_request_endpoint, 
                                        headers=self.headers, 
                                        params=self.params)
            if response.status_code == 202:
                self.logger.info(f"Ожидание данных от App Metrica {i*wait_s} сек.: {response.text}")
                time.sleep(wait_s)
                continue
            elif response.status_code == 200:
                self.logger.info(f"Успешно получены данные из App Metrica через {i*wait_s}")
                return response.json()
            else:
                self.logger.error(f"Ошибка при отправке запроса: {response.status_code=}, {response.text}")
                return False
        raise TimeoutError(f'Ошибка при получении данных из App Metrica: данные не поступили за {n_retries * wait_s} сек.')

