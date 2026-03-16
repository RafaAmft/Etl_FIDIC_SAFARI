"""
Testes para conversor de datas.
"""
import pytest

from src.extraction.date_converter import DateConverter


class TestDateConverter:
    """Testes do DateConverter."""

    def test_convert_mm_yyyy(self) -> None:
        """Testa conversão de MM/YYYY."""
        assert DateConverter.convert_to_iso("12/2024") == "2024-12"
        assert DateConverter.convert_to_iso("01/2025") == "2025-01"
        assert DateConverter.convert_to_iso("6/2023") == "2023-06"

    def test_convert_dd_mm_yyyy(self) -> None:
        """Testa conversão de DD/MM/YYYY."""
        assert DateConverter.convert_to_iso("15/12/2024") == "2024-12"
        assert DateConverter.convert_to_iso("01/01/2025") == "2025-01"
        assert DateConverter.convert_to_iso("31/06/2023") == "2023-06"

    def test_convert_yyyy_mm(self) -> None:
        """Testa conversão de YYYY/MM."""
        assert DateConverter.convert_to_iso("2024/12") == "2024-12"
        assert DateConverter.convert_to_iso("2025/01") == "2025-01"

    def test_invalid_dates(self) -> None:
        """Testa datas inválidas."""
        assert DateConverter.convert_to_iso("") is None
        assert DateConverter.convert_to_iso("0") is None
        assert DateConverter.convert_to_iso("0.0") is None
        assert DateConverter.convert_to_iso(None) is None
        assert DateConverter.convert_to_iso("nan") is None
        assert DateConverter.convert_to_iso("None") is None

    def test_invalid_month(self) -> None:
        """Testa mês inválido."""
        assert DateConverter.convert_to_iso("13/2024") is None
        assert DateConverter.convert_to_iso("00/2024") is None
        assert DateConverter.convert_to_iso("15/2024") is None

    def test_invalid_year(self) -> None:
        """Testa ano fora do range configurado."""
        assert DateConverter.convert_to_iso("12/1999") is None
        assert DateConverter.convert_to_iso("12/2036") is None
        assert DateConverter.convert_to_iso("12/2050") is None

    def test_with_fallback(self) -> None:
        """Testa conversão com fallback."""
        assert DateConverter.convert_with_fallback("", "12/2024") == "2024-12"
        assert DateConverter.convert_with_fallback("01/2025", "12/2024") == "2025-01"
        assert DateConverter.convert_with_fallback(None, None) is None
        assert DateConverter.convert_with_fallback(None, "06/2024") == "2024-06"

    def test_format_without_slash_returns_none(self) -> None:
        """Formato sem '/' deve retornar None."""
        assert DateConverter.convert_to_iso("2024-12") is None
        assert DateConverter.convert_to_iso("202412") is None

    def test_ambiguous_format_returns_none(self) -> None:
        """Formato ambíguo (não MM/YYYY nem YYYY/MM) deve retornar None."""
        assert DateConverter.convert_to_iso("12/24") is None
