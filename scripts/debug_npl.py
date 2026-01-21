"""
Script para investigar problema no cálculo de NPL.
"""

import pandas as pd

# Carregar dados
df = pd.read_csv('outputs/cleaned_snapshot.csv', sep=';', decimal=',')

print('=== ANÁLISE DE NPL ===')
print(f'Total registros: {len(df)}')

# Buscar registros com NPL muito alto
npl_altos = df[df['INDICE_NPL_DECIMAL'] > 100].copy()
print(f'\nRegistros com NPL > 100: {len(npl_altos)}')

if len(npl_altos) > 0:
    print('\n=== TOP 5 NPLs MAIS ALTOS ===')
    for idx, row in npl_altos.nlargest(5, 'INDICE_NPL_DECIMAL').iterrows():
        cnpj = row['CNPJ_FUNDO']
        npl_gravado = row['INDICE_NPL_DECIMAL']
        inad = row['INADIMPLENCIA_TOTAL']
        cart_bruta = row['CARTEIRA_BRUTA']
        
        print(f'\n{idx+1}. CNPJ: {cnpj}')
        print(f'   NPL gravado: {npl_gravado:,.2f}')
        print(f'   Inadimplência: R$ {inad:,.2f}')
        print(f'   Carteira Bruta: R$ {cart_bruta:,.2f}')
        
        if cart_bruta > 0 and inad > 0:
            npl_esperado = inad / cart_bruta
            print(f'   NPL esperado: {npl_esperado:.12f} (decimal)')
            print(f'   NPL esperado: {npl_esperado*100:.10f}%')
            
            if npl_esperado > 0:
                fator = npl_gravado / npl_esperado
                print(f'   >>> ERRO: NPL multiplicado por {fator:,.0f}x <<<')


# Verificar outros casos extremos
print('\n=== OUTROS CASOS COM NPL ALTO ===')
npl_altos = df[df['INDICE_NPL_DECIMAL'] > 100].copy()
print(f'Total de registros com NPL > 100: {len(npl_altos)}')

if len(npl_altos) > 0:
    print('\nTop 5 NPLs mais altos:')
    for idx, row in npl_altos.nlargest(5, 'INDICE_NPL_DECIMAL').iterrows():
        print(f'  CNPJ: {row["CNPJ_FUNDO"]}')
        print(f'    NPL gravado: {row["INDICE_NPL_DECIMAL"]:,.2f}')
        print(f'    Inadimpl: {row["INADIMPLENCIA_TOTAL"]:,.2f}')
        print(f'    Carteira: {row["CARTEIRA_BRUTA"]:,.2f}')
        if row["CARTEIRA_BRUTA"] > 0:
            esperado = row["INADIMPLENCIA_TOTAL"] / row["CARTEIRA_BRUTA"]
            print(f'    NPL esperado: {esperado:.10f}')
        print()
