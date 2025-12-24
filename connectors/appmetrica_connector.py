import requests
import logging

from utils import get_logger


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

    def test_connection(self) -> bool:
        """Проверка соединения с App Metrica"""
        test_result = False
        test_endpoint = '/management/v1/applications'
        headers = {
            'Authorization': 'OAuth ' + self.auth_token
        }
        r = self.session.get(self.base_url + test_endpoint, headers=headers)
        if r.status_code == 200:
            test_result = True
            self.logger.info(f"Успешное подключение к App Metrica")
        else:
            self.logger.error(f"Ошибка при подключение к App Metrica: {r.response}")
        
        return test_result
