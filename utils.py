import logging
import sys

def get_logger(logger_name: str, log_level: int) -> logging.Logger:
    """Настройка логгера с выводом в консоль"""
    # logger = logging.getLogger(f"ClickHouseConnector.{self.host}")
    logger = logging.getLogger(logger_name)
    
    # Если у логгера уже есть обработчики, не добавляем новые
    if not logger.handlers:
        logger.setLevel(log_level)
        
        # Создаем обработчик для консоли
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        
        # Создаем форматтер
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        # Добавляем обработчик к логгеру
        logger.addHandler(console_handler)
        
        # Предотвращаем дублирование сообщений через родительские логгеры
        logger.propagate = False
    
    return logger