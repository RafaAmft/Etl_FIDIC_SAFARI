# Dicionário de Colunas — ETL FIDC

**Arquivo de referência:** `cleaned_snapshot_YYYYMMDD_HHMMSS.csv`
**Total de colunas:** 103
**Separador:** `;` (ponto-e-vírgula)
**Decimal:** `,` (vírgula)
**Encoding:** UTF-8 com BOM
**Fonte dos dados:** B3 FNET — Informe Mensal de FIDC (XML)

---

## 1. Identificação do Fundo

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 1 | `CNPJ_FUNDO` | `str` | CNPJ do fundo FIDC (14 dígitos, sem formatação) |
| 2 | `CNPJ_ADMINISTRADOR` | `str` | CNPJ do administrador do fundo |
| 3 | `DATA_COMPETENCIA` | `str` | Mês/ano de referência do informe (formato MM/AAAA) |
| 4 | `TIPO_CONDOMINIO` | `str` | Tipo de condomínio: `Aberto` ou `Fechado` |
| 5 | `FUNDO_EXCLUSIVO` | `str` | Indica se o fundo é exclusivo (`S`/`N`) |
| 6 | `CLASSE_UNICA` | `str` | Indica se o fundo possui classe única de cotas (`S`/`N`) |
| 7 | `COTISTA_VINCULADO` | `str` | Indica se há cotista vinculado ao cedente (`S`/`N`) |

---

## 2. Ativos Gerais

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 8 | `ATIVO_TOTAL` | `float` | Valor total dos ativos do fundo (R$) |
| 9 | `DISPONIBILIDADES` | `float` | Caixa e equivalentes de caixa disponíveis (R$) |
| 10 | `CARTEIRA_TOTAL` | `float` | Valor total da carteira de crédito (R$) |
| 11 | `OUTROS_ATIVOS_TOTAL` | `float` | Total de outros ativos não classificados nas demais categorias (R$) |
| 12 | `OUTROS_ATIVOS_CURTO_PRAZO` | `float` | Parcela de curto prazo dos outros ativos (R$) |
| 13 | `OUTROS_ATIVOS_LONGO_PRAZO` | `float` | Parcela de longo prazo dos outros ativos (R$) |

---

## 3. Créditos Existentes

> Créditos diretos adquiridos pelo fundo, principal base para cálculo de inadimplência.

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 14 | `CREDITOS_ADQUIRIDOS` | `float` | Total de créditos adquiridos pelo fundo (R$) |
| 15 | `CRED_VENCIDOS_ADIMPLENTES` | `float` | Créditos vencidos e ainda adimplentes (R$) |
| 16 | `CRED_VENCIDOS_INADIMPLENTES` | `float` | Créditos vencidos e inadimplentes (R$) |
| 17 | `CRED_TOTAL_VENC_INADIMPL` | `float` | Total de créditos vencidos inadimplentes (R$) |
| 18 | `CRED_INADIMPLENCIA` | `float` | Valor de inadimplência dos créditos existentes (R$) |
| 19 | `CRED_PERFORMADOS` | `float` | Créditos a vencer (performados / dentro do prazo) (R$) |
| 20 | `CRED_VENCIDOS_PENDENTES` | `float` | Créditos vencidos com situação pendente de classificação (R$) |
| 21 | `CRED_EMP_RECUPERACAO` | `float` | Créditos em processo de recuperação (R$) |
| 22 | `CRED_RECEITA_PUBLICA` | `float` | Créditos de receita pública (R$) |
| 23 | `CRED_ACAO_JUDICIAL` | `float` | Créditos em ação judicial (R$) |
| 24 | `CRED_CONSTITUICAO_JURIDICA` | `float` | Créditos em constituição jurídica (R$) |
| 25 | `CRED_PROVISAO_REDUCAO` | `float` | Provisão para redução de valor dos créditos (R$) — valor negativo |

---

## 4. Direitos Creditórios — DICRED

> Versão alternativa da carteira de crédito reportada em campo DICRED do XML.

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 26 | `DICRED_TOTAL` | `float` | Total dos direitos creditórios — DICRED (R$) |
| 27 | `DICRED_CEDENTE` | `float` | Direitos creditórios com cedente (R$) |
| 28 | `DICRED_VENC_INADIMPL` | `float` | DICRED vencidos e inadimplentes (R$) |
| 29 | `DICRED_TOTAL_VENC_INAD` | `float` | Total DICRED vencidos inadimplentes (R$) |
| 30 | `DICRED_INADIMPLENCIA` | `float` | Inadimplência dos DICRED (R$) |
| 31 | `DICRED_PERFORMADOS` | `float` | DICRED a vencer (performados) (R$) |
| 32 | `DICRED_VENC_PENDENTES` | `float` | DICRED vencidos pendentes (R$) |
| 33 | `DICRED_EMP_RECUPERACAO` | `float` | DICRED em recuperação (R$) |
| 34 | `DICRED_RECEITA_PUBLICA` | `float` | DICRED de receita pública (R$) |
| 35 | `DICRED_ACAO_JUDICIAL` | `float` | DICRED em ação judicial (R$) |
| 36 | `DICRED_PROVISAO_REDUCAO` | `float` | Provisão para redução dos DICRED (R$) — valor negativo |

---

## 5. Valores Mobiliários

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 37 | `VALORES_MOBILIARIOS_TOTAL` | `float` | Total de valores mobiliários na carteira (R$) |
| 38 | `DEBENTURES` | `float` | Posição em debêntures (R$) |
| 39 | `CRI` | `float` | Certificados de Recebíveis Imobiliários (R$) |
| 40 | `NOTAS_PROMISSORIAS_COMERCIAIS` | `float` | Notas promissórias comerciais (R$) |
| 41 | `LETRAS_FINANCEIRAS` | `float` | Letras financeiras (R$) |
| 42 | `COTAS_FIF` | `float` | Cotas de Fundos de Investimento Financeiro (R$) |
| 43 | `OUTROS_DIREITOS_CREDITORIOS` | `float` | Outros direitos creditórios não classificados acima (R$) |

---

## 6. Outros Ativos Financeiros

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 44 | `TITULOS_PUBLICOS_FEDERAIS` | `float` | Títulos públicos federais (LFT, NTN-B, etc.) (R$) |
| 45 | `CDB` | `float` | Certificados de Depósito Bancário (R$) |
| 46 | `APLICACOES_COMPROMISSADAS` | `float` | Operações compromissadas (R$) |
| 47 | `ATIVOS_FINANCEIROS_RF` | `float` | Demais ativos financeiros de renda fixa (R$) |
| 48 | `COTAS_FIDC` | `float` | Cotas de outros FIDCs (R$) |

---

## 7. Mercado de Derivativos

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 49 | `DERIVATIVOS_TOTAL` | `float` | Valor total de posições em derivativos (R$) |
| 50 | `TERMO_COMPRADOR` | `float` | Posição compradora em contratos a termo (R$) |
| 51 | `OPCOES_TITULAR` | `float` | Opções na posição de titular (R$) |
| 52 | `FUTUROS_AJUSTE_POSITIVO` | `float` | Ajuste positivo em contratos futuros (R$) |
| 53 | `SWAP_A_RECEBER` | `float` | Swap com valor a receber (ponta ativa) (R$) |
| 54 | `COBERTURA_PRESTADA` | `float` | Coberturas prestadas (R$) |
| 55 | `DEPOSITOS_MARGEM` | `float` | Depósitos de margem em garantia (R$) |

---

## 8. Segmentação da Carteira

> Decomposição da carteira por setor econômico. A soma deve ser igual a `CARTEIRA_SEGMENTADA_TOTAL`.

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 56 | `CARTEIRA_SEGMENTADA_TOTAL` | `float` | Soma total de todos os segmentos abaixo (R$) |

### 8.1 Setores Gerais
| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 57 | `SEGMT_INDUSTRIAL` | `float` | Créditos do setor industrial (R$) |
| 58 | `SEGMT_MERCADO_IMOBILIARIO` | `float` | Créditos do mercado imobiliário (R$) |
| 59 | `SEGMT_AGRONEGOCIO` | `float` | Créditos do agronegócio (R$) |
| 60 | `SEGMT_CARTAO_CREDITO` | `float` | Créditos originados de cartão de crédito (R$) |
| 61 | `SEGMT_ACAO_JUDICIAL` | `float` | Créditos em ação judicial / precatórios judiciais (R$) |
| 62 | `SEGMT_PROPRIEDADE_INTELECTUAL` | `float` | Créditos de propriedade intelectual / royalties (R$) |

### 8.2 Segmento Comercial
| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 63 | `SEGMT_COMERCIAL_TOTAL` | `float` | Total do segmento comercial (R$) |
| 64 | `SEGMT_COMERCIO` | `float` | Créditos de comércio em geral (R$) |
| 65 | `SEGMT_COMERCIO_VAREJO` | `float` | Créditos de comércio varejista (R$) |
| 66 | `SEGMT_ARREND_MERCANTIL` | `float` | Créditos de arrendamento mercantil (leasing) (R$) |

### 8.3 Segmento de Serviços
| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 67 | `SEGMT_SERVICOS_TOTAL` | `float` | Total do segmento de serviços (R$) |
| 68 | `SEGMT_SERVICOS_GERAIS` | `float` | Créditos de serviços em geral (R$) |
| 69 | `SEGMT_SERVICOS_PUBLICOS` | `float` | Créditos de serviços públicos (R$) |
| 70 | `SEGMT_SERVICOS_EDUCACAO` | `float` | Créditos do setor de educação (R$) |
| 71 | `SEGMT_SERVICOS_ENTRETENIMENTO` | `float` | Créditos do setor de entretenimento (R$) |

### 8.4 Segmento Financeiro
| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 72 | `SEGMT_FINANCEIRO_TOTAL` | `float` | Total do segmento financeiro (R$) |
| 73 | `SEGMT_FINANC_CREDITO_PESSOA` | `float` | Crédito pessoal / pessoa física (R$) |
| 74 | `SEGMT_FINANC_CONSIGNADO` | `float` | Crédito consignado (R$) |
| 75 | `SEGMT_FINANC_CORPORATIVO` | `float` | Crédito corporativo / grandes empresas (R$) |
| 76 | `SEGMT_FINANC_MIDDLE_MARKET` | `float` | Crédito middle market / médias empresas (R$) |
| 77 | `SEGMT_FINANC_VEICULOS` | `float` | Crédito para aquisição de veículos (R$) |
| 78 | `SEGMT_FINANC_IMOB_EMPRESARIAL` | `float` | Crédito imobiliário empresarial (R$) |
| 79 | `SEGMT_FINANC_IMOB_RESIDENCIAL` | `float` | Crédito imobiliário residencial (R$) |
| 80 | `SEGMT_FINANC_OUTROS` | `float` | Outros créditos financeiros não classificados (R$) |

### 8.5 Segmento Factoring
| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 81 | `SEGMT_FACTORING_TOTAL` | `float` | Total do segmento de factoring (R$) |
| 82 | `SEGMT_FACTORING_PESSOA` | `float` | Factoring pessoa física (R$) |
| 83 | `SEGMT_FACTORING_CORPORATIVO` | `float` | Factoring pessoa jurídica / corporativo (R$) |

### 8.6 Setor Público
| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 84 | `SEGMT_SETOR_PUBLICO_TOTAL` | `float` | Total de créditos do setor público (R$) |
| 85 | `SEGMT_PRECATORIOS` | `float` | Precatórios judiciais (R$) |
| 86 | `SEGMT_CREDITOS_TRIBUTARIOS` | `float` | Créditos tributários (R$) |
| 87 | `SEGMT_ROYALTIES` | `float` | Royalties (R$) |
| 88 | `SEGMT_SETOR_PUBLICO_OUTROS` | `float` | Outros créditos do setor público (R$) |

---

## 9. Indicadores Calculados

> Calculados pelo ETL com base nos campos extraídos do XML.

| # | Coluna | Tipo | Fórmula | Descrição |
|---|--------|------|---------|-----------|
| 89 | `INADIMPLENCIA_TOTAL` | `float` | `CRED_INADIMPLENCIA + DICRED_INADIMPLENCIA` | Total consolidado de inadimplência (R$) |
| 90 | `INDICE_NPL_DECIMAL` | `float` | `INADIMPLENCIA_TOTAL / CREDITOS_ADQUIRIDOS` | Índice de inadimplência (NPL) em decimal (0–1) |
| 91 | `INDICE_NPL_PERCENTUAL` | `float` | `INDICE_NPL_DECIMAL × 100` | Índice de inadimplência (NPL) em percentual (0–100) |
| 92 | `TAXA_LIQUIDEZ_PERCENTUAL` | `float` | `(DISPONIBILIDADES / ATIVO_TOTAL) × 100` | Liquidez imediata como % do ativo total |
| 93 | `CONCENTRACAO_CREDITO_PERCENTUAL` | `float` | `(CREDITOS_ADQUIRIDOS / ATIVO_TOTAL) × 100` | Concentração da carteira de crédito no ativo total |
| 99 | `CARTEIRA_BRUTA` | `float` | `CREDITOS_ADQUIRIDOS + DICRED_TOTAL` | Carteira bruta consolidada (R$) |
| 100 | `TAXA_LIQUIDEZ_DECIMAL` | `float` | `DISPONIBILIDADES / ATIVO_TOTAL` | Liquidez imediata em decimal (0–1) |
| 101 | `CONCENTRACAO_CREDITO_DECIMAL` | `float` | `CREDITOS_ADQUIRIDOS / ATIVO_TOTAL` | Concentração de crédito em decimal (0–1) |

---

## 10. Metadados de Processamento

> Informações sobre o processo de extração e o documento fonte.

| # | Coluna | Tipo | Descrição |
|---|--------|------|-----------|
| 94 | `STATUS` | `str` | Resultado do processamento — ver tabela de status abaixo |
| 95 | `DATA_REFERENCIA_DOC` | `str` | Data de referência do documento na B3 |
| 96 | `ID_DOCUMENTO` | `str` | Identificador único do documento XML na B3 |
| 97 | `MENSAGEM_ERRO` | `str` | Mensagem de erro em caso de falha (vazio se `STATUS = SUCESSO`) |

### Valores possíveis de `STATUS`

| Valor | Significado |
|-------|-------------|
| `SUCESSO` | Processamento completo sem erros |
| `ERRO_BUSCA` | Falha ao buscar documentos na API da B3 |
| `SEM_INFORME_MENSAL` | Nenhum informe mensal encontrado para o CNPJ |
| `ERRO_DOWNLOAD` | Falha ao baixar o XML do documento |
| `ERRO_PARSE` | Falha ao interpretar o conteúdo do XML |
| `ERRO_INESPERADO` | Erro não previsto durante o processamento |

---

## 11. Flags de Qualidade (QA)

> Sinalizadores booleanos para anomalias detectadas automaticamente. `True` indica problema.

| # | Coluna | Tipo | Condição de ativação | Significado |
|---|--------|------|----------------------|-------------|
| 101 | `ATIVO_ZERO_FLAG` | `bool` | `ATIVO_TOTAL == 0` | Fundo reportou ativo total zerado — possível dado ausente ou fundo inativo |
| 102 | `CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG` | `bool` | `CARTEIRA_BRUTA == 0` e `INADIMPLENCIA_TOTAL > 0` | Carteira zerada mas com inadimplência — inconsistência de dados |
| 103 | `SEM_POSICAO_FLAG` | `bool` | `CARTEIRA_TOTAL == 0` e `ATIVO_TOTAL > 0` | Fundo tem ativos mas não reporta posição de crédito |

> **Nota:** O modelo prevê mais 2 flags (`DIVERGE_LIQ_FLAG` e `DIVERGE_NPL_FLAG`) ainda não implementadas na validação.

---

## Resumo por Categoria

| Categoria | Qtd. Colunas |
|-----------|-------------|
| Identificação do Fundo | 7 |
| Ativos Gerais | 6 |
| Créditos Existentes | 12 |
| Direitos Creditórios (DICRED) | 11 |
| Valores Mobiliários | 7 |
| Outros Ativos Financeiros | 5 |
| Mercado de Derivativos | 7 |
| Segmentação da Carteira | 33 |
| Indicadores Calculados | 8 |
| Metadados de Processamento | 4 |
| Flags de Qualidade (QA) | 3 |
| **TOTAL** | **103** |
