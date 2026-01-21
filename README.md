# ETL FIDC - Extração de Dados de Fundos FIDC

Sistema modular de ETL para extração, transformação e validação de dados de Fundos de Investimento em Direitos Creditórios (FIDC) da B3.

## 🎯 Características

- ✅ **90+ campos extraídos** de XMLs da API B3
- ✅ **5 validações de QA** automatizadas
- ✅ **Indicadores financeiros** calculados (NPL, Liquidez, Concentração)
- ✅ **Relatórios automáticos** (CSV/Excel)
- ✅ **Comparação de versões** de dados
- ✅ **Logging estruturado** para auditoria
- ✅ **Arquitetura modular** e testável

## 📁 Estrutura do Projeto

```
.
├── src/                      # Código fonte principal
│   ├── config/              # Configurações (API, paths, etc)
│   ├── models/              # Modelos de dados (FIDCData, Flags)
│   ├── extractors/          # API B3 + XML Parser v1.0.2
│   ├── transformers/        # Limpeza e cálculo de indicadores
│   ├── validators/          # Validações QA + Diff Generator
│   ├── loaders/             # Exportação CSV/Excel
│   ├── services/            # Orquestração (ETL + QA)
│   └── utils/               # Logging e utilitários
├── scripts/                  # Scripts CLI
│   ├── run_etl.py           # ⭐ Script principal
│   └── compare_versions.py  # Comparação de versões
├── data/                     # Dados de entrada
├── outputs/                  # Resultados gerados
└── notebooks/                # Notebooks originais

## 🚀 Instalação

```bash
# Clonar repositório
cd "c:\Projetos\Proejto FIDIC SAFARI"

# Instalar dependências
pip install -r requirements.txt
```

## 📊 Uso

### 1. Executar ETL Completo

```bash
python scripts/run_etl.py
```

**O que faz:**
1. Carrega CNPJs de `data/lista_cnpjs_fidc.csv`
2. Busca e baixa XMLs da API B3
3. Extrai 90+ campos financeiros
4. Calcula indicadores (NPL, Liquidez)
5. Aplica 5 validações de QA
6. Gera relatórios em `outputs/`

**Saídas geradas:**
- `outputs/cleaned_snapshot.csv` - Dados completos validados
- `outputs/qa_issues.csv` - Registros com problemas
- `outputs/etl_fidc.log` - Log detalhado

### 2. Comparar Versões

```bash
python scripts/compare_versions.py outputs/snapshot1.csv outputs/snapshot2.csv
```

Gera: `outputs/diff_v1_v2.csv` com diferenças numéricas

### 3. Analisar Qualidade dos Dados

```bash
python scripts/analyze_data_quality.py
```

**O que faz:**
1. Analisa estatísticas gerais (sucesso, erros, completude)
2. Avalia flags de QA (quantos registros com cada problema)
3. Calcula estatísticas de indicadores financeiros (NPL, Liquidez, Ativo)
4. Detecta outliers (NPL alto, ativos extremos)
5. Lista top fundos por NPL

**Saída gerada:**
- `outputs/data_quality_report.txt` - Relatório completo de qualidade

## 📈 Validações de QA

O sistema aplica 5 flags de validação automaticamente:

| Flag | Descrição |
|------|-----------|
| `ATIVO_ZERO_FLAG` | Ativo Total = 0 |
| `DIVERGE_LIQ_FLAG` | Divergência entre liquidez calc. e informada |
| `CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG` | Carteira vazia com inadimplência |
| `DIVERGE_NPL_FLAG` | Divergência entre NPL calc. e informado |
| `SEM_POSICAO_FLAG` | Sem posição de crédito mas com ativo |

## 🔧 Uso Programático

```python
from src.services.etl_service import FIDCETLService
from src.services.qa_service import QAService

# ETL
etl = FIDCETLService()
df = etl.process_and_validate(['51199121000145', '47388724000118'])

# QA
qa = QAService()
results = qa.full_qa_pipeline(df, output_dir='outputs')
```

## 📝 Formato de Entrada

O arquivo `data/lista_cnpjs_fidc.csv` deve conter:

```csv
CNPJ,NOME_FUNDO,CNPJ_ORIGINAL
51199121000145,2MONEY RESP LIMITADA FIDC NP SUBORDINADA JÚNIOR 1,51.199.121/0001-45
47388724000118,3R RESP LIMITADA FIDC NP ÚNICA 1,47.388.724/0001-18
```

## 🧪 Desenvolvimento

```bash
# Formatar código
black src/ scripts/

# Verificar estilo
flake8 src/ scripts/

# Executar testes (quando disponíveis)
pytest tests/
```

## 📄 Licença

Projeto interno - Rafael Augusto © 2026

## 🤝 Contribuindo

Para contribuir:
1. Mantenha a estrutura modular
2. Documente todas as funções
3. Use type hints
4. Adicione testes unitários
5. Atualize este README

## 📞 Suporte

Ver documentação em `docs/` para detalhes técnicos.
