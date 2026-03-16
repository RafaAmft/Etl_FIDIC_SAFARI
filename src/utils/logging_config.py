"""
Configuração de Logging centralizada e padronizada.

Recursos:
- Logs em arquivo e console
- Rotação de arquivos (limite 10MB, mantém 5 backups)
- Formato detalhado com timestamp, nível e módulo

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(
    log_level: str = "INFO",
    log_file: str = "etl_fidc.log"
):
    """
    Configura o sistema de logging da aplicação.
    
    Args:
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR)
        log_file: Caminho do arquivo de log
    """
    # Garantir que diretório existe
    log_path = Path(log_file)
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except:
        pass

    # Formato unificado
    log_format = '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, date_format)

    # Handler de Arquivo com Rotação (Max 10MB per file, keep 5)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024, # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    # Handler de Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Configuração Root Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Limpar handlers anteriores para evitar duplicação
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Silenciar logs muito verbosos de bibliotecas externas
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logging.info(f"Logging configurado. Nível: {log_level}, Arquivo: {log_file}")
