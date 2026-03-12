# FIDC ETL - Monitor de Fundos Distressed

> Sistema ETL para coleta, análise e monitoramento de Fundos de Investimento em Direitos Creditórios (FIDCs) com foco em identificação de oportunidades de distressed assets.

## ✨ Funcionalidades Principais

- ✅ **Coleta automatizada** de dados da B3 (rolling 60 meses)
- ✅ **Extração de 100+ campos** por fundo
- ✅ **Cálculo de métricas** de distress (NPL, Liquidez, Zumbi Ratio)
- ✅ **Sistema de scoring** FIDC Safari
- ✅ **Detecção automática** de anomalias estruturais
- ✅ **Cache inteligente** com versionamento
- ✅ **Arquitetura modular** e testável

## 📦 Instalação

```bash
# 1. Clonar repositório
git clone <repo-url>
cd Etl_Fidic

# 2. Criar ambiente virtual
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# 3. Instalar dependências
pip install -r requirements.txt

# 4. (Opcional) Instalar dependências de desenvolvimento
pip install -r requirements-dev.txt
```

## 🚀 Uso

### Executar ETL Completo

```bash
# Via módulo (recomendado)
python -m src

# Via script wrapper
python scripts\run_etl.py
```

### Limpar Cache

```bash
python scripts\clear_cache.py
```

## 📊 Estrutura dos Outputs

```
data/output/2025-12-26/
├── fidc_monitor_completo_20251226_235900.csv      # Todos os fundos
├── fidc_safari_oportunidades_20251226_235900.csv  # Score >= 30
└── fidc_distressed_npl_gt_20_20251226_235900.csv  # NPL >= 20%
```

## 📈 Métricas Calculadas

### NPL Ratio (Non-Performing Loans)
```
NPL = (Créditos Vencidos Inadimplentes / Carteira Total) × 100
```

**Classificação:**
- 0-5%: Saudável
- 5-15%: Atenção
- 15-50%: Distressed
- 50-100%: Severo
- \>100%: Crítico (anomalia estrutural)

### FIDC Safari Score (0-100)

**Componentes:**
- **NPL (40%)**: Maior NPL = maior oportunidade
- **Zumbi Ratio (25%)**: Créditos > 3 anos vencidos
- **Liquidez (25%)**: Menor liquidez = maior pressão
- **Porte (10%)**: Ativo total do fundo

**Classificação:**
- 70-100: Alta oportunidade
- 50-70: Oportunidade moderada
- 30-50: Monitorar
- <30: Sem oportunidade

## 🏗️ Arquitetura

```
src/
├── core/           # Configurações e logging
├── api/            # Cliente B3 e cache
├── extractors/     # Extração de XML
├── transformers/   # Métricas e scoring
├── loaders/        # Export de dados
└── main.py         # Pipeline principal
```

## 🧪 Testes

```bash
# Rodar todos os testes
pytest

# Com cobertura
pytest --cov=src --cov-report=html
```

## ⚙️ Configuração

Edite `src/core/config.py` para ajustar:
- Rolling window (padrão: 60 meses)
- Thresholds de filtros (NPL, Score)
- Cache (habilitar/desabilitar)
- Timeouts e retry

## 📄 Licença

MIT License

## 🤝 Contribuindo

Pull requests são bem-vindos! Para mudanças maiores, abra uma issue primeiro.

---

**Versão:** 8.0.0 (Refatorado + Otimizado)  
**Última atualização:** 2026-03-12
