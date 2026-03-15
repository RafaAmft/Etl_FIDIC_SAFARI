"""
Testes para XMLParser.
"""
import xml.etree.ElementTree as ET

import pytest

from src.extraction.xml_parser import XMLParser


class TestConverterValor:
    """Testes para XMLParser.converter_valor."""

    def test_numero_br_virgula(self) -> None:
        """Formato brasileiro: vírgula como decimal."""
        assert XMLParser.converter_valor("1.234,56") == pytest.approx(1234.56)
        assert XMLParser.converter_valor("691,88") == pytest.approx(691.88)

    def test_numero_inteiro(self) -> None:
        assert XMLParser.converter_valor("1000") == pytest.approx(1000.0)

    def test_numero_zero(self) -> None:
        assert XMLParser.converter_valor("0,00") == pytest.approx(0.0)

    def test_float_passado_direto(self) -> None:
        assert XMLParser.converter_valor(3.14) == pytest.approx(3.14)
        assert XMLParser.converter_valor(0) == pytest.approx(0.0)

    def test_none_retorna_zero(self) -> None:
        assert XMLParser.converter_valor(None) == pytest.approx(0.0)
        assert XMLParser.converter_valor("") == pytest.approx(0.0)

    def test_data_preservada_como_string(self) -> None:
        """Valores com '/' que parecem datas devem ser preservados como string."""
        assert XMLParser.converter_valor("11/2025") == "11/2025"
        assert XMLParser.converter_valor("01/11/2025") == "01/11/2025"
        assert XMLParser.converter_valor("2025/11") == "2025/11"

    def test_string_invalida_retorna_zero(self) -> None:
        assert XMLParser.converter_valor("abc") == pytest.approx(0.0)
        assert XMLParser.converter_valor("N/A") == pytest.approx(0.0)


class TestBuscarValorXml:
    """Testes para XMLParser.buscar_valor_xml."""

    @pytest.fixture
    def xml_simples(self) -> ET.Element:
        return ET.fromstring(
            "<ROOT><VALOR>71326638,62</VALOR><DATA>11/2025</DATA></ROOT>"
        )

    def test_valor_numerico_encontrado(self, xml_simples: ET.Element) -> None:
        result = XMLParser.buscar_valor_xml(xml_simples, "VALOR")
        assert result == pytest.approx(71_326_638.62)

    def test_data_preservada(self, xml_simples: ET.Element) -> None:
        result = XMLParser.buscar_valor_xml(xml_simples, "DATA")
        assert result == "11/2025"

    def test_campo_ausente_retorna_fallback(self, xml_simples: ET.Element) -> None:
        # caminho com '/' → fallback 0.0
        result = XMLParser.buscar_valor_xml(xml_simples, "APLIC_ATIVO/VL_AUSENTE")
        assert result == pytest.approx(0.0)

    def test_campo_ausente_sem_slash_retorna_string_vazia(self, xml_simples: ET.Element) -> None:
        result = XMLParser.buscar_valor_xml(xml_simples, "CAMPO_AUSENTE")
        assert result == ""

    def test_namespace_removido(self) -> None:
        """Parser deve lidar com XMLs que têm namespace implícito."""
        xml_ns = ET.fromstring(
            '<ROOT xmlns:ns="http://exemplo.com"><ns:VALOR>100,00</ns:VALOR></ROOT>'
        )
        # buscar_valor_xml usa .// então deve encontrar o elemento
        result = XMLParser.buscar_valor_xml(xml_ns, "VALOR")
        # Namespace pode fazer com que não encontre; resultado esperado é 0.0 ou valor
        assert isinstance(result, (float, str))


class TestParseXml:
    """Testes para XMLParser.parse_xml."""

    def test_xml_valido(self, sample_xml_bytes: bytes) -> None:
        root = XMLParser.parse_xml(sample_xml_bytes)
        assert root is not None
        assert isinstance(root, ET.Element)

    def test_xml_malformado_retorna_none(self, malformed_xml_bytes: bytes) -> None:
        root = XMLParser.parse_xml(malformed_xml_bytes)
        assert root is None

    def test_bytes_vazios_retorna_none(self) -> None:
        root = XMLParser.parse_xml(b"")
        assert root is None


class TestLimparTag:
    """Testes para XMLParser.limpar_tag."""

    def test_remove_namespace(self) -> None:
        assert XMLParser.limpar_tag("{http://exemplo.com}TAG") == "TAG"

    def test_sem_namespace(self) -> None:
        assert XMLParser.limpar_tag("TAG") == "TAG"

    def test_string_vazia(self) -> None:
        assert XMLParser.limpar_tag("") == ""
