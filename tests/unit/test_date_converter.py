"""
Testes para conversor de datas.
"""
import pytest
from src.extractors.date_converter import DateConverter


class TestDateConverter:
    """Testes do DateConverter"""
    
    def test_convert_mm_yyyy(self):
        """Testa conversão de MM/YYYY"""
        assert DateConverter.convert_to_iso("12/2024") == "2024-12"
        assert DateConverter.convert_to_iso("01/2025") == "2025-01"
        assert DateConverter.convert_to_iso("6/2023") == "2023-06"  # Com zero padding
    
    def test_convert_dd_mm_yyyy(self):
        """Testa conversão de DD/MM/YYYY"""
        assert DateConverter.convert_to_iso("15/12/2024") == "2024-12"
        assert DateConverter.convert_to_iso("01/01/2025") == "2025-01"
        assert DateConverter.convert_to_iso("31/06/2023") == "2023-06"
    
    def test_convert_yyyy_mm(self):
        """Testa conversão de YYYY/MM"""
        assert DateConverter.convert_to_iso("2024/12") == "2024-12"
        assert DateConverter.convert_to_iso("2025/01") == "2025-01"
    
    def test_invalid_dates(self):
        """Testa datas inválidas"""
        assert DateConverter.convert_to_iso("") is None
        assert DateConverter.convert_to_iso("0") is None
        assert DateConverter.convert_to_iso("0.0") is None
        assert DateConverter.convert_to_iso(None) is None
        assert DateConverter.convert_to_iso("nan") is None
    
    def test_invalid_month(self):
        """Testa mês inválido"""
        assert DateConverter.convert_to_iso("13/2024") is None
        assert DateConverter.convert_to_iso("00/2024") is None
        assert DateConverter.convert_to_iso("15/2024") is None
    
    def test_invalid_year(self):
        """Testa ano inválido"""
        assert DateConverter.convert_to_iso("12/1999") is None
        assert DateConverter.convert_to_iso("12/2031") is None
        assert DateConverter.convert_to_iso("12/2050") is None
    
    def test_with_fallback(self):
        """Testa conversão com fallback"""
        result = DateConverter.convert_with_fallback("", "12/2024")
        assert result == "2024-12"
        
        result = DateConverter.convert_with_fallback("01/2025", "12/2024")
        assert result == "2025-01"  # Usa o primário
        
        result = DateConverter.convert_with_fallback(None, None)
        assert result is None
