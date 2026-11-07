"""
Testes para FieldExtractor.
"""
from typing import Any, Dict

import pytest

from src.extraction.field_extractor import FieldExtractor


class TestExtractAllFields:
    """Testes para FieldExtractor.extract_all_fields."""

    @pytest.fixture
    def extractor(self) -> FieldExtractor:
        return FieldExtractor()

    def test_extrai_campos_basicos(
        self, extractor: FieldExtractor, sample_xml_bytes: bytes
    ) -> None:
        dados = extractor.extract_all_fields(sample_xml_bytes)

        assert "CNPJ_FUNDO" in dados
        assert "DATA_COMPETENCIA" in dados
        assert "ATIVO_TOTAL" in dados
        assert "CARTEIRA_TOTAL" in dados
        assert "PATRIMONIO_LIQUIDO" in dados
        assert "PASSIVO_CIRCULANTE" in dados

    def test_cnpj_extraido_corretamente(
        self, extractor: FieldExtractor, sample_xml_bytes: bytes
    ) -> None:
        dados = extractor.extract_all_fields(sample_xml_bytes)
        # O sample XML tem CNPJ 31405473000100
        import re
        assert re.sub(r"\.0$", "", str(dados["CNPJ_FUNDO"])) == "31405473000100"

    def test_data_competencia_convertida_para_iso(
        self, extractor: FieldExtractor, sample_xml_bytes: bytes
    ) -> None:
        dados = extractor.extract_all_fields(sample_xml_bytes)
        # DT_COMPT = "11/2025" → DATA_COMPETENCIA = "2025-11"
        assert dados["DATA_COMPETENCIA"] == "2025-11"

    def test_valores_numericos_convertidos(
        self, extractor: FieldExtractor, sample_xml_bytes: bytes
    ) -> None:
        dados = extractor.extract_all_fields(sample_xml_bytes)
        # CARTEIRA_TOTAL = 71326638,62
        assert isinstance(dados["CARTEIRA_TOTAL"], float)
        assert dados["CARTEIRA_TOTAL"] == pytest.approx(71_326_638.62)

    def test_cedentes_concentrados_inicializados(
        self, extractor: FieldExtractor, sample_xml_bytes: bytes
    ) -> None:
        dados = extractor.extract_all_fields(sample_xml_bytes)
        assert "CEDENTES_CONCENTRADOS_JSON" in dados
        assert "QTD_CEDENTES_CONCENTRADOS" in dados
        # Sample XML não tem cedentes com > 10% PL
        assert dados["QTD_CEDENTES_CONCENTRADOS"] == 0

    def test_xml_invalido_levanta_value_error(self, extractor: FieldExtractor) -> None:
        with pytest.raises(ValueError, match="parse do XML"):
            extractor.extract_all_fields(b"<broken xml")

    def test_xml_vazio_levanta_value_error(self, extractor: FieldExtractor) -> None:
        with pytest.raises(ValueError):
            extractor.extract_all_fields(b"")

    def test_todos_campos_aging_presentes(
        self, extractor: FieldExtractor, sample_xml_bytes: bytes
    ) -> None:
        dados = extractor.extract_all_fields(sample_xml_bytes)
        aging_campos = [
            "AGING_VENC_1_30_DIAS", "AGING_VENC_31_60_DIAS", "AGING_VENC_61_90_DIAS",
            "AGING_VENC_91_120_DIAS", "AGING_VENC_MAIOR_1080_DIAS",
        ]
        for campo in aging_campos:
            assert campo in dados, f"Campo ausente: {campo}"
            assert isinstance(dados[campo], (int, float))
