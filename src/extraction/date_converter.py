"""
Conversor de datas do formato brasileiro (MM/YYYY, DD/MM/YYYY) para ISO (YYYY-MM).
"""
import logging
from typing import Optional

from src.utils.constants import YEAR_MAX, YEAR_MIN

logger = logging.getLogger(__name__)

_NULL_VALUES = frozenset({"", "0", "0.0", "None", "nan"})


class DateConverter:
    """Conversor de datas para formato ISO."""

    @staticmethod
    def convert_to_iso(date_str: Optional[str]) -> Optional[str]:
        """
        Converte data de formato brasileiro para ISO (YYYY-MM).

        Formatos suportados:
        - MM/YYYY  → YYYY-MM
        - DD/MM/YYYY → YYYY-MM
        - YYYY/MM  → YYYY-MM

        Args:
            date_str: Data em string (ex: "12/2024", "01/12/2024").

        Returns:
            str: Data no formato YYYY-MM, ou None se inválida.

        Examples:
            >>> DateConverter.convert_to_iso("12/2024")
            '2024-12'
            >>> DateConverter.convert_to_iso("01/12/2024")
            '2024-12'
        """
        if not date_str:
            return None

        normalized = str(date_str).strip()
        if normalized in _NULL_VALUES:
            return None

        if "/" not in normalized:
            return None

        try:
            parts = normalized.split("/")

            if len(parts) == 2:
                part1, part2 = parts
                if len(part2) == 4:      # MM/YYYY
                    month, year = part1, part2
                elif len(part1) == 4:    # YYYY/MM
                    year, month = part1, part2
                else:
                    logger.warning(f"Formato de data ambíguo: {normalized!r}")
                    return None

            elif len(parts) == 3:
                # DD/MM/YYYY
                _, month, year = parts

            else:
                logger.warning(f"Formato de data não suportado: {normalized!r}")
                return None

            month_int = int(month)
            year_int = int(year)

            if not (1 <= month_int <= 12):
                logger.warning(f"Mês inválido: {month_int} em '{normalized}'")
                return None

            if not (YEAR_MIN <= year_int <= YEAR_MAX):
                logger.warning(f"Ano fora do range [{YEAR_MIN}-{YEAR_MAX}]: {year_int}")
                return None

            return f"{year_int}-{month_int:02d}"

        except (ValueError, AttributeError) as exc:
            logger.warning(f"Erro ao converter data '{normalized}': {exc}")
            return None

    @staticmethod
    def convert_with_fallback(
        primary_date: Optional[str],
        fallback_date: Optional[str],
    ) -> Optional[str]:
        """
        Tenta converter data primária; se falhar, usa fallback.

        Args:
            primary_date: Data principal.
            fallback_date: Data de fallback.

        Returns:
            str: Data convertida, ou None.
        """
        result = DateConverter.convert_to_iso(primary_date)

        if result is None and fallback_date:
            result = DateConverter.convert_to_iso(fallback_date)
            if result:
                logger.debug(f"Usando data de fallback: {fallback_date!r} → {result!r}")

        return result
