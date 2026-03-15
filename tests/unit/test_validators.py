"""
Testes para validadores Pydantic v2 e funções de validação de CNPJ.
"""
import pytest
from pydantic import ValidationError

from src.utils.validators import (
    CNPJInput,
    ExportFilter,
    FundoData,
    _cnpj_checksum,
    validate_cnpj,
)


# CNPJs válidos reais para testes
CNPJ_VALIDO = "31405473000100"        # CNPJ real válido
CNPJ_VALIDO_FORMATADO = "31.405.473/0001-00"
CNPJ_VALIDO_2 = "12345678000195"     # outro CNPJ válido


class TestCnpjChecksum:
    """Testes para a função _cnpj_checksum."""

    def test_cnpj_valido_retorna_true(self) -> None:
        assert _cnpj_checksum(CNPJ_VALIDO) is True

    def test_cnpj_valido_2_retorna_true(self) -> None:
        assert _cnpj_checksum(CNPJ_VALIDO_2) is True

    def test_cnpj_digito_errado_retorna_false(self) -> None:
        cnpj_errado = CNPJ_VALIDO[:-1] + "9"  # último dígito errado
        assert _cnpj_checksum(cnpj_errado) is False

    def test_cnpj_todos_zeros_retorna_false(self) -> None:
        """CNPJs com todos os dígitos iguais são inválidos."""
        assert _cnpj_checksum("00000000000000") is False

    def test_cnpj_todos_uns_retorna_false(self) -> None:
        assert _cnpj_checksum("11111111111111") is False

    def test_cnpj_todos_noves_retorna_false(self) -> None:
        assert _cnpj_checksum("99999999999999") is False


class TestValidateCnpj:
    """Testes para a função validate_cnpj."""

    def test_cnpj_valido_retorna_apenas_digitos(self) -> None:
        result = validate_cnpj(CNPJ_VALIDO)
        assert result == CNPJ_VALIDO
        assert result.isdigit()

    def test_cnpj_formatado_retorna_apenas_digitos(self) -> None:
        result = validate_cnpj(CNPJ_VALIDO_FORMATADO)
        assert result == CNPJ_VALIDO

    def test_cnpj_com_espacos_e_pontos(self) -> None:
        cnpj_com_formatacao = "31.405.473/0001-00"
        result = validate_cnpj(cnpj_com_formatacao)
        assert result == CNPJ_VALIDO
        assert len(result) == 14

    def test_cnpj_curto_levanta_value_error(self) -> None:
        with pytest.raises(ValueError, match="14 dígitos"):
            validate_cnpj("123456")

    def test_cnpj_com_14_digitos_mas_invalido(self) -> None:
        cnpj_14_invalido = "00000000000000"
        with pytest.raises(ValueError):
            validate_cnpj(cnpj_14_invalido)

    def test_cnpj_com_digito_verificador_errado(self) -> None:
        # Últimos 2 dígitos do CNPJ_VALIDO são "00"; trocar por "99" torna inválido
        cnpj_errado = CNPJ_VALIDO[:-2] + "99"
        with pytest.raises(ValueError, match="dígitos verificadores"):
            validate_cnpj(cnpj_errado)

    def test_cnpj_vazio_levanta_value_error(self) -> None:
        with pytest.raises(ValueError):
            validate_cnpj("")


class TestCNPJInputModel:
    """Testes para o schema Pydantic CNPJInput."""

    def test_cnpj_valido_instancia_modelo(self) -> None:
        model = CNPJInput(cnpj=CNPJ_VALIDO)
        assert model.cnpj == CNPJ_VALIDO

    def test_cnpj_formatado_normalizado(self) -> None:
        model = CNPJInput(cnpj=CNPJ_VALIDO_FORMATADO)
        assert model.cnpj == CNPJ_VALIDO

    def test_cnpj_invalido_levanta_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            CNPJInput(cnpj="00000000000000")

    def test_cnpj_curto_levanta_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            CNPJInput(cnpj="123")


class TestFundoDataModel:
    """Testes para o schema Pydantic FundoData."""

    def test_instancia_com_campos_minimos(self) -> None:
        model = FundoData(CNPJ_FUNDO=CNPJ_VALIDO)
        assert model.CNPJ_FUNDO == CNPJ_VALIDO
        assert model.ATIVO_TOTAL == 0.0
        assert model.NPL_RATIO == 0.0
        assert model.ANOMALIA_ESTRUTURAL is False

    def test_campos_extras_sao_permitidos(self) -> None:
        model = FundoData(
            CNPJ_FUNDO=CNPJ_VALIDO,
            CAMPO_CUSTOMIZADO="valor_extra",
            OUTRO_CAMPO=42,
        )
        assert model.model_extra["CAMPO_CUSTOMIZADO"] == "valor_extra"

    def test_data_competencia_opcional(self) -> None:
        model = FundoData(CNPJ_FUNDO=CNPJ_VALIDO)
        assert model.DATA_COMPETENCIA is None

    def test_coercion_numerica(self) -> None:
        """Pydantic v2 deve converter string numérica para float."""
        model = FundoData(CNPJ_FUNDO=CNPJ_VALIDO, ATIVO_TOTAL="1000000.0")
        assert model.ATIVO_TOTAL == pytest.approx(1_000_000.0)


class TestExportFilterModel:
    """Testes para o schema Pydantic ExportFilter."""

    def test_valores_padrao(self) -> None:
        f = ExportFilter()
        assert f.score_min == pytest.approx(30.0)
        assert f.npl_min == pytest.approx(20.0)
        assert f.ativo_min == pytest.approx(0.0)

    def test_valores_personalizados(self) -> None:
        f = ExportFilter(score_min=50.0, npl_min=25.0, ativo_min=1_000_000.0)
        assert f.score_min == pytest.approx(50.0)

    def test_valor_negativo_levanta_erro(self) -> None:
        with pytest.raises(ValidationError):
            ExportFilter(score_min=-1.0)

    def test_zero_e_valido(self) -> None:
        f = ExportFilter(score_min=0.0, npl_min=0.0)
        assert f.score_min == pytest.approx(0.0)
