"""
Parser XML para documentos FIDC da B3.
"""
import logging
import xml.etree.ElementTree as ET
from typing import Union, Optional

logger = logging.getLogger(__name__)


class XMLParser:
    """Parser de XML com tratamento de namespaces."""

    @staticmethod
    def limpar_tag(tag: str) -> str:
        """Remove namespace XML das tags."""
        return tag.split("}")[-1] if "}" in tag else tag

    @staticmethod
    def converter_valor(texto: Union[str, float, int, None]) -> Union[float, str]:
        """
        Converte string no formato brasileiro para float com validação robusta.
        Preserva strings com padrão de data (contendo '/').

        Args:
            texto: Valor a ser convertido.

        Returns:
            float: Valor convertido, ou 0.0 se inválido.
            str: String original se for padrão de data.
        """
        if texto is None or texto == "":
            return 0.0

        if isinstance(texto, (int, float)):
            return float(texto)

        try:
            texto_str = str(texto).strip()
            if not texto_str:
                return 0.0

            # CRÍTICO: Detectar padrão de data e preservar como string.
            # Formatos: MM/YYYY, DD/MM/YYYY, YYYY/MM
            if "/" in texto_str:
                partes = texto_str.split("/")
                if len(partes) in (2, 3):
                    try:
                        for parte in partes:
                            int(parte)
                        return texto_str  # É uma data válida
                    except ValueError:
                        pass  # Não é data, tentar converter para número

            # Remove pontos (milhar) e troca vírgula por ponto (decimal BR)
            texto_limpo = texto_str.replace(".", "").replace(",", ".")
            return float(texto_limpo)

        except (ValueError, AttributeError) as exc:
            logger.debug(f"Erro ao converter '{texto}': {exc}")
            return 0.0

    @staticmethod
    def buscar_valor_xml(root: ET.Element, caminho: str) -> Union[float, str]:
        """
        Busca um valor no XML com tratamento robusto de namespaces.

        Args:
            root: Elemento raiz do XML.
            caminho: Caminho XPath simplificado.

        Returns:
            float ou string, com fallback para 0.0 ou ''.
        """
        fallback: Union[float, str] = 0.0 if "/" in caminho else ""

        try:
            elemento = root.find(f".//{caminho}")
            if elemento is None or not elemento.text:
                return fallback

            texto = elemento.text.strip()
            if not texto:
                return fallback

            try:
                return XMLParser.converter_valor(texto)
            except (ValueError, TypeError, AttributeError):
                # Não foi possível converter — retornar como string
                return texto

        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Erro ao buscar '{caminho}': {exc}")
            return fallback

    @staticmethod
    def parse_xml(xml_content: bytes) -> Optional[ET.Element]:
        """
        Faz parse de conteúdo XML.

        Args:
            xml_content: Conteúdo XML em bytes.

        Returns:
            ET.Element: Raiz do XML, ou None se erro.
        """
        try:
            return ET.fromstring(xml_content)
        except ET.ParseError as exc:
            logger.error(f"Erro ao fazer parse do XML: {exc}")
            return None
