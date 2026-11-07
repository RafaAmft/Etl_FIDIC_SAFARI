"""
Script para executar o ETL completo.
"""
import sys
from pathlib import Path

# Adicionar diretório raiz do projeto ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.main import main

if __name__ == "__main__":
    main()
