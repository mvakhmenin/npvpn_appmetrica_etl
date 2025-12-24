import pandas as pd
from typing import Optional, Union, List, Dict
import logging
import sys
import clickhouse_connect
from clickhouse_connect.driver.client import Client

from utils import get_logger

class ClickHouseConnector:
    """Коннектор для ClickHouse с использованием clickhouse-connect"""
    
    def __init__(self, 
                 host: str, 
                 port: int,
                 username: str, 
                 password: str,
                 secure: bool = True,
                 verify: bool = False,
                 ca_cert='ca.crt',
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
            self.client = None
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
    
    def insert_dataframe(self, 
                         table_name: str, 
                         df: pd.DataFrame,
                         database: Optional[str] = None,
                         column_names: Optional[List[str]] = None,
                         settings: Optional[Dict] = None,
                         **kwargs):
        """
        Вставка данных из DataFrame в таблицу ClickHouse
        
        Args:
            table_name: Имя таблицы
            df: DataFrame с данными
            database: Имя базы данных (если None, используется self.database)
            column_names: Имена колонок для вставки (если None, используются все колонки DataFrame)
            settings: Дополнительные настройки для вставки
            **kwargs: Дополнительные аргументы для client.insert_df()
        """
        if not self.client:
            raise ConnectionError("Сначала выполните подключение через метод connect()")
        
        if df.empty:
            self.logger.warning("DataFrame пуст, вставка не требуется")
            return
        
        try:
            # Подготовка данных - обработка типов
            df_prepared = self._prepare_dataframe(df)
            
            # Определяем имена колонок
            if column_names is None:
                column_names = list(df_prepared.columns)
            
            # Определяем базу данных
            target_database = database or self.database
            full_table_name = f"{target_database}.{table_name}" if target_database else table_name
            
            # Вставляем данные
            self.client.insert_df(table=full_table_name, 
                                  df=df_prepared, 
                                  column_names=column_names,
                                  settings=settings,
                                  **kwargs)
            
            self.logger.info(f"Успешно вставлено {len(df_prepared)} строк в таблицу {full_table_name}")
            
        except Exception as e:
            self.logger.error(f"Ошибка вставки данных: {e}")
            raise
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Подготовка DataFrame для вставки в ClickHouse
        
        Args:
            df: Исходный DataFrame
            
        Returns:
            Подготовленный DataFrame
        """
        df_prepared = df.copy()
        
        # Обработка типов данных
        for col in df_prepared.columns:
            # Преобразование datetime64[ns] в datetime
            if pd.api.types.is_datetime64_any_dtype(df_prepared[col]):
                # Конвертируем в timezone-naive datetime
                df_prepared[col] = pd.to_datetime(df_prepared[col]).dt.tz_localize(None)
            
            # Замена бесконечных значений на None
            if pd.api.types.is_float_dtype(df_prepared[col]):
                df_prepared[col] = df_prepared[col].replace([np.inf, -np.inf], None)
            
            # Замена NaN на None (для корректной вставки NULL)
            df_prepared[col] = df_prepared[col].where(pd.notnull(df_prepared[col]), None)
        
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