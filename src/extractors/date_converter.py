"""
Conversor de datas do formato brasileiro (MM/YYYY, DD/MM/YYYY) para ISO (YYYY-MM).
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DateConverter:
    """Conversor de datas para formato ISO"""
    
    @staticmethod
    def convert_to_iso(date_str: Optional[str]) -> Optional[str]:
        """
        Converte data de formato brasileiro para ISO (YYYY-MM).
        
        Formatos suportados:
        - MM/YYYY → YYYY-MM
        - DD/MM/YYYY → YYYY-MM
        - YYYY/MM → YYYY-MM
        
        Args:
            date_str: Data em string (ex: "12/2024", "01/12/2024")
            
        Returns:
            str: Data no formato YYYY-MM ou None se inválida
            
        Examples:
            >>> DateConverter.convert_to_iso("12/2024")
            "2024-12"
            
            >>> DateConverter.convert_to_iso("01/12/2024")
            "2024-12"
        """
        if not date_str or str(date_str).strip() in ['', '0', '0.0', 'None', 'nan']:
            return None
        
        try:
            date_str = str(date_str).strip()
            
            if '/' not in date_str:
                return None
            
            parts = date_str.split('/')
            
            if len(parts) == 2:
                # Formato: MM/YYYY ou YYYY/MM
                part1, part2 = parts
                
                if len(part2) == 4:  # MM/YYYY
                    month, year = part1, part2
                elif len(part1) == 4:  # YYYY/MM
                    year, month = part1, part2
                else:
                    logger.warning(f"Formato ambíguo: {date_str}")
                    return None
                
            elif len(parts) == 3:
                # Formato: DD/MM/YYYY
                _, month, year = parts
            else:
                logger.warning(f"Formato não suportado: {date_str}")
                return None
            
            # Validar e formatar
            month_int = int(month)
            year_int = int(year)
            
            if not (1 <= month_int <= 12):
                logger.warning(f"Mês inválido: {month_int}")
                return None
            
            if not (2000 <= year_int <= 2030):
                logger.warning(f"Ano fora do range: {year_int}")
                return None
            
            return f"{year_int}-{month_int:02d}"
            
        except (ValueError, AttributeError) as e:
            logger.warning(f"Erro ao converter data '{date_str}': {e}")
            return None
    
    @staticmethod
    def convert_with_fallback(
        primary_date: Optional[str],
        fallback_date: Optional[str]
    ) -> Optional[str]:
        """
        Tenta converter data primária, se falhar usa fallback.
        
        Args:
            primary_date: Data principal
            fallback_date: Data de fallback
            
        Returns:
            str: Data convertida ou None
        """
        result = DateConverter.convert_to_iso(primary_date)
        
        if result is None and fallback_date:
            result = DateConverter.convert_to_iso(fallback_date)
            if result:
                logger.debug(f"Usando data de fallback: {fallback_date} → {result}")
        
        return result
