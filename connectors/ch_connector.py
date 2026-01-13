import pandas as pd
from typing import Optional, Union, List, Dict
import logging
import sys
import clickhouse_connect
from clickhouse_connect.driver.client import Client

from utils import get_logger
from config import appmetrica_ch_tables

class ClickHouseConnector:
    """Коннектор для ClickHouse с использованием clickhouse-connect"""
    
    def __init__(self, 
                 host: str, 
                 port: int,
                 username: str, 
                 password: str,
                 secure: bool = True,
                 verify: bool = False,
                 ca_cert='ca_crt',
                 compress: bool = True,
                 log_level: int = logging.WARNING):
        """
        Инициализация параметров подключения
        
        Args:
            host: Хост ClickHouse сервера
            port: HTTP порт 
            username: Имя пользователя
            password: Пароль
            secure: Использовать SSL/TLS
            verify: проверка сертификата
            ca_cert: путь до сертификата
            compress: Включить сжатие данных
            log_level: уровень логирования
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.secure = secure
        self.verify = verify
        self.ca_cert = ca_cert
        self.compress = compress
        
        self.client: Optional[Client] = None
        self.logger = get_logger(f"ClickHouseConnector.{self.host}", log_level)
        
    def connect(self) -> bool:
        """
        Установка подключения к ClickHouse
        
        Returns:
            bool: True если подключение успешно, False в противном случае
        """
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                secure=self.secure,
                verify=self.verify,
                ca_cert=self.ca_cert,
                compress=self.compress
            )
            
            # Проверяем подключение
            result = self.client.query("SELECT 1 as check")
            if result.first_row[0] == 1:
                self.logger.info(f"Успешное подключение к ClickHouse {self.host}:{self.port}")
                return True
            else:
                raise ConnectionError("Не удалось проверить подключение")
                
        except Exception as e:
            self.logger.error(f"Ошибка подключения к ClickHouse: {e}")
            self.disconnect()
            return False
    
    def disconnect(self):
        """Закрытие подключения"""
        if self.client:
            self.client.close()
            self.client = None
            self.logger.info("Подключение к ClickHouse закрыто")
    
    def execute_query(self, 
                      query: str, 
                      return_df: bool = True) -> Union[pd.DataFrame, List[Dict]]:
        """
        Выполнение запроса к ClickHouse
        
        Args:
            query: SQL запрос
            return_df: Возвращать результат как DataFrame
            
        Returns:
            Результат запроса в виде DataFrame или списка словарей (named_results)
        """
        if not self.client:
            raise ConnectionError("Сначала выполните подключение через метод connect()")
        
        try:
            if return_df:
                # Преобразуем в DataFrame
                result = self.client.query_df(query)
            else:
                result = self.client.query(query)
                result = [row for row in result.named_results()]
            return result
                
        except Exception as e:
            self.logger.error(f"Ошибка выполнения запроса: {e}")
            raise
    
    def get_target_max_date(self, target: str):
        """
        Получение максимальной даты данных в источнике (installations или events)
        Необходмо для последующего запроса актуальных данных из App Metrica 

        Args:
            target: источник (installations или events)

        Returns:
            Дата в формате datetime.datetime
        
        """
        target_table = appmetrica_ch_tables[target]['table_name']
        date_time_field = appmetrica_ch_tables[target]['date_time_field']

        target_max_date_sql = f"""
                SELECT MAX({date_time_field})
                FROM {target_table}
            """
        self.logger.info(f"Получаю MAX {date_time_field} из таблицы {target_table}")
        sql_res = self.execute_query(target_max_date_sql, return_df=False)
        max_date = list(sql_res[0].values())[0]
        self.logger.info(f"MAX {date_time_field} из таблицы {target_table}: {max_date.strftime('%Y-%m-%d %H:%M:%S')}")
        return max_date

    def insert_source_data(self,
                           source_name: str,
                           df: pd.DataFrame,
                         ):
        """
        Docstring for insert_source_data
        
        :param self: Description
        :param source_name: Description
        :type source_name: str
        :param df: Description
        :type df: pd.DataFrame
        """
        table_name = appmetrica_ch_tables[source_name]['table_name']
        self.insert_dataframe(table_name, df)
        return

    def insert_dataframe(self, 
                         table_name: str, 
                         df: pd.DataFrame,
                         ):
        """
        Вставка данных из DataFrame в таблицу ClickHouse
        
        Args:
            table_name: Имя таблицы
            df: DataFrame с данными
        """
        if not self.client:
            raise ConnectionError("Сначала выполните подключение через метод connect()")
        
        if df.empty:
            self.logger.warning("DataFrame пуст, вставка не требуется")
            return
        
        try:
            # Подготовка данных - обработка типов
            df_prepared = self._prepare_dataframe(df)
            
            # Вставляем данные
            self.client.insert_df(table=table_name, 
                                  df=df_prepared
                                  )
            
            self.logger.info(f"Успешно вставлено {len(df)} строк в таблицу {table_name}")
            
        except Exception as e:
            self.logger.error(f"Ошибка вставки данных в таблицу {table_name}: {e}")
            raise
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Подготовка DataFrame для вставки в ClickHouse 
                -- преобразование типов в соответствии с целевой таблицей Clickhouse
        
        Args:
            df: Исходный DataFrame
            
        Returns:
            Подготовленный DataFrame
        """
        df_prepared = df.copy()
        
        int_columns = ['application_id', 
                       'click_timestamp',
                       'tracking_id',
                       'install_receive_timestamp',
                       'mcc',
                       'mnc',
                       'event_receive_timestamp',
                       'event_timestamp',
                       'app_build_number']
        datetime_columns = ['click_datetime',
                            'install_datetime',
                            'install_receive_datetime',
                            'event_datetime',
                            'event_receive_datetime']
        bool_columns = ['is_reattribution', 
                        'is_reinstallation']

        for col in df_prepared.columns:
            if col in int_columns:
                df_prepared[col] = df_prepared[col].astype(int)
            elif col in datetime_columns:
                df_prepared[col] = pd.to_datetime(df_prepared[col], format='%Y-%m-%d %H:%M:%S')
            elif col in bool_columns:
                df_prepared[col] = df_prepared[col].map({
                                    'true': 1,
                                    'false': 0
                                }).fillna(0).astype(bool)
        
        return df_prepared
    
    def test_connection(self) -> bool:
        """Проверка соединения с ClickHouse"""
        try:
            if not self.client:
                return False
            result = self.client.query("SELECT 1 as test")
            return bool(result and result.first_row[0] == 1)
        except Exception as e:
            self.logger.error(f"Ошибка проверки соединения: {e}")
            return False