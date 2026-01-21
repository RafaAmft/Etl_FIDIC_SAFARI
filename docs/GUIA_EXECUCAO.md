# 🚀 Guia de Execução - FIDC SAFARI

## ✅ Checklist Pré-Execução

- [x] Dependências instaladas (pandas, requests, openpyxl)
- [x] Projeto organizado com estrutura de diretórios
- [x] Arquivo `data/lista_cnpjs_fidc.csv` presente (441 CNPJs)
- [ ] Conexão com internet estável (para API B3)
- [ ] ~15 minutos disponíveis para processamento completo

## 📋 Opção 1: Execução via Jupyter Notebook (Recomendado)

### Passo 1: Abrir Jupyter
```bash
cd "c:\Projetos\Proejto FIDIC SAFARI"
jupyter notebook
```

### Passo 2: Navegar e Executar
1. No navegador, abrir: `notebooks/etl_fidic_vfinal.ipynb`
2. Menu → **Cell** → **Run All**
3. Aguardar conclusão (~15 minutos)

### Passo 3: Monitorar Execução
- Checkpoints automáticos a cada 50 CNPJs
- Progresso exibido: `[X/441] CNPJ - NOME_FUNDO... ✅ OK`
- Arquivo checkpoint: `outputs/checkpoint_temp.csv`

## 📋 Opção 2: Execução via VSCode (com Jupyter Extension)

### Passo 1: Abrir no VSCode
```bash
cd "c:\Projetos\Proejto FIDIC SAFARI"
code .
```

### Passo 2: Executar Notebook
1. Abrir `notebooks/etl_fidic_vfinal.ipynb`
2. Selecionar kernel Python
3. Clicar em **Run All** (ou Shift+Enter em cada célula)

## 📋 Opção 3: Conversão para Script Python

### Converter Notebook para .py
```bash
jupyter nbconvert --to script notebooks/etl_fidic_vfinal.ipynb --output-dir scripts
```

### Executar Script
```bash
python scripts/etl_fidic_vfinal.py
```

## 🔧 Ajustes Necessários no Notebook

O notebook atual usa caminhos relativos que precisam ser ajustados:

### Antes (linha do notebook):
```python
ARQUIVO_CSV_ENTRADA = 'lista_cnpjs_fidc.csv'
```

### Depois (recomendado):
```python
import os
BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')
OUTPUTS_DIR = os.path.join(BASE_DIR, '..', 'outputs')
ARQUIVO_CSV_ENTRADA = os.path.join(DATA_DIR, 'lista_cnpjs_fidc.csv')
```

**OU** executar notebook a partir do diretório raiz do projeto.

## 📊 Outputs Esperados

Ao final da execução, serão criados em `outputs/`:

1. **base_fidc_completa.csv** - Todos os registros
2. **base_fidc_sucesso.csv** - Apenas sucessos
3. **base_fidc_erros.csv** - Apenas erros (se houver)
4. **base_fidc_completa.xlsx** - Excel com 3 abas

## 📈 Estatísticas Esperadas

Após processamento:
- Total CNPJs: 441
- Taxa de sucesso: ~90-95%
- Tempo de execução: 14-16 minutos
- Registros gerados: ~450-500 (alguns CNPJs têm múltiplos fundos)

## ⚠️ Troubleshooting

### Erro: `FileNotFoundError: lista_cnpjs_fidc.csv`
**Solução**: Execute o notebook a partir do diretório raiz:
```bash
cd "c:\Projetos\Proejto FIDIC SAFARI"
jupyter notebook notebooks/etl_fidic_vfinal.ipynb
```

### Erro: `ModuleNotFoundError: No module named 'pandas'`
**Solução**: Instale dependências:
```bash
pip install -r requirements.txt
```

### Timeout/Bloqueio da API B3
**Solução**: 
- Delay já configurado (2s entre requisições)
- Se persistir, aumentar `DELAY_ENTRE_REQUISICOES` para 3s

### Interrupção Durante Execução
**Solução**: 
- Use checkpoint salvo: `outputs/checkpoint_temp.csv`
- Identifique último CNPJ processado
- Ajuste CSV de entrada para continuar de onde parou

## 🎯 Próximos Passos Após Execução

1. Verificar outputs em `outputs/`
2. Revisar estatísticas impressas no notebook
3. Comparar com CSV histórico (`RAW/fidc_monitor_completo_20251226_224926.csv`)
4. Analisar fundos com alto NPL (>5%)
5. Importar dados no Power BI / Tableau / Excel para dashboards

## 📝 Notas Importantes

- **Não interromper**: Processo cria arquivos temporários que precisam ser limpos
- **API pública**: Sem necessidade de autenticação, mas respeite rate limits
- **Dados públicos**: Informações são públicas da B3, mas verifique termos de uso
