"""
Configuração de logging do projeto FIDC ETL.
"""
import logging
import sys
from pathlib import Path
from typing import Optional

# Bibliotecas externas que produzem logs excessivos
_NOISY_LOGGERS = ("urllib3", "requests", "httpcore", "httpx")


def setup_logging(log_file: Optional[Path] = None, level: int = logging.INFO) -> None:
    """
    Configura o sistema de logging do projeto.

    Args:
        log_file: Caminho para arquivo de log (opcional).
        level: Nível de logging (default: INFO).
    """
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, date_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remover handlers existentes para evitar duplicatas
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Handler para console (stderr é convencional para logs)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Handler para arquivo (se especificado)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except OSError as exc:
            root_logger.warning(f"Não foi possível criar arquivo de log {log_file}: {exc}")

    # Suprimir logs verbosos de bibliotecas externas
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
