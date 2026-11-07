"""
Script para limpar o cache do ETL.
"""
import sys
from pathlib import Path

# Adicionar diretório raiz do projeto ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.cache import CacheManager
from src.core.logging_config import setup_logging

if __name__ == "__main__":
    setup_logging()
    
    cache = CacheManager()
    print("🧹 Limpando cache...")
    count = cache.clear_all()
    print(f"✅ {count} arquivos de cache removidos!")
