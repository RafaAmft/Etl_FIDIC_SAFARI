# 📊 Estrutura do Projeto FIDC SAFARI

```
c:\Projetos\Proejto FIDIC SAFARI\
│
├── 📁 data/                              # Dados de entrada
│   └── lista_cnpjs_fidc.csv             # 441 CNPJs para processar
│
├── 📁 notebooks/                         # Jupyter Notebooks
│   ├── etl_fidic_vfinal.ipynb           # ⭐ ETL Principal (USAR ESTE)
│   └── Untitled10.ipynb                 # ETL versão anterior (legado)
│
├── 📁 scripts/                           # Scripts Python
│   └── etl_fidc.py                      # Template de script (em preparo)
│
├── 📁 outputs/                           # Resultados gerados
│   └── checkpoint_temp.csv              # Checkpoint de execução anterior
│   │
│   └── (Após execução serão criados:)
│       ├── base_fidc_completa.csv       # Todos os registros
│       ├── base_fidc_sucesso.csv        # Apenas sucessos
│       ├── base_fidc_erros.csv          # Apenas erros
│       └── base_fidc_completa.xlsx      # Excel (3 abas)
│
├── 📁 RAW/                               # Dados brutos/históricos
│   └── fidc_monitor_completo_20251226_224926.csv  # CSV histórico (795 registros)
│
├── 📁 docs/                              # Documentação
│   ├── GUIA_EXECUCAO.md                 # 📘 Guia de execução detalhado
│   └── ESTRUTURA.md                     # 📘 Este arquivo
│
├── 📄 README.md                          # Documentação principal do projeto
└── 📄 requirements.txt                   # Dependências Python
```

## 🎯 Próximos Passos

### 1️⃣ **Abrir Notebook** (Opção recomendada)
```bash
cd "c:\Projetos\Proejto FIDIC SAFARI"
jupyter notebook
```
Depois navegar até: `notebooks/etl_fidic_vfinal.ipynb`

### 2️⃣ **Executar ETL**
- Menu → Cell → Run All
- Ou execute célula por célula (Shift+Enter)

### 3️⃣ **Aguardar Conclusão**
- Tempo estimado: ~15 minutos
- 441 CNPJs serão processados
- Checkpoints automáticos a cada 50 CNPJs

### 4️⃣ **Verificar Outputs**
Os resultados estarão em: `outputs/`

## 📊 Dados de Entrada

**Arquivo**: `data/lista_cnpjs_fidc.csv`
- **Total**: 441 CNPJs únicos
- **Colunas**: CNPJ, NOME_FUNDO, CNPJ_ORIGINAL
- **Formato**: UTF-8 com BOM

## 📈 Outputs Esperados

| Arquivo | Descrição | Registros Esperados |
|---------|-----------|---------------------|
| `base_fidc_completa.csv` | Todos (sucessos + erros) | ~450-500 |
| `base_fidc_sucesso.csv` | Apenas processados com sucesso | ~400-440 |
| `base_fidc_erros.csv` | Apenas erros (se houver) | ~0-40 |
| `base_fidc_completa.xlsx` | Excel (3 abas) | ~450-500 |

> **Nota**: Alguns CNPJs podem ter múltiplos fundos, por isso o número de registros pode ser maior que 441.

## 🔍 Campos Extraídos (90+)

### Categorias Principais:
- ✅ **Identificação** (7 campos)
- ✅ **Ativos** (6 campos)
- ✅ **Créditos Existentes** (12 campos) - foco em inadimplência
- ✅ **Direitos Creditórios** (11 campos)
- ✅ **Valores Mobiliários** (6 campos)
- ✅ **Derivativos** (7 campos)
- ✅ **Segmentação** (40+ campos) - industrial, financeiro, etc
- ✅ **Indicadores Calculados** (3 campos) - NPL%, Liquidez%, Concentração%

## ⚙️ Configurações da API B3

| Parâmetro | Valor | Observação |
|-----------|-------|------------|
| **Delay entre requisições** | 2s | Evita bloqueio |
| **Timeout busca** | 10s | Pesquisa de documentos |
| **Timeout download** | 20s | Download de XMLs |
| **Limite por busca** | 200 docs | Por CNPJ |

## 🛠️ Dependências

Instalar com:
```bash
pip install -r requirements.txt
```

**Lista**:
- pandas >= 2.0.0
- requests >= 2.31.0
- openpyxl >= 3.1.0

## 📚 Documentação Disponível

| Arquivo | Conteúdo |
|---------|----------|
| `README.md` | Visão geral completa do projeto |
| `docs/GUIA_EXECUCAO.md` | Guia passo a passo de execução |
| `docs/ESTRUTURA.md` | Este arquivo - estrutura do projeto |
| `requirements.txt` | Lista de dependências |

## ✅ Status do Projeto

- ✅ Estrutura organizada
- ✅ Documentação completa
- ✅ Dados de entrada validados (441 CNPJs)
- ✅ Dependências listadas
- ✅ Notebook pronto para execução
- ⏳ **Aguardando execução do ETL**

## 🎓 Dicas

1. **Primeira execução**: Leia `docs/GUIA_EXECUCAO.md` antes de começar
2. **Monitoramento**: Acompanhe o progresso no console do notebook
3. **Checkpoints**: Não interrompa no meio - use checkpoints se necessário
4. **Comparação**: Compare outputs com `RAW/fidc_monitor_completo_20251226_224926.csv`

---

**Projeto preparado e pronto para execução! 🚀**
