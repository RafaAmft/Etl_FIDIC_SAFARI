import sys
import os
import logging

# Adicionar diretório raiz ao path para encontrar o pacote src
sys.path.append(os.getcwd())

import pandas as pd
import numpy as np
from src.transformers.data_cleaner import convert_numeric_columns

# Configurar logging básico
logging.basicConfig(level=logging.INFO)

def teste_reproducao():
    print("=== INICIANDO TESTE DE REPRODUÇÃO ===")
    
    # 1. Simular valor vindo do XML Parser (float correto)
    # INAD: 0.01
    # CARTEIRA: 31740113.80
    npl_float = 0.01 / 31740113.80  # 3.150587317614e-10
    
    print(f"\n1. Valor Original (Float Python):")
    print(f"   {npl_float}")
    print(f"   {npl_float:.20f}")
    
    # 2. Criar DataFrame (simulando etl_service)
    data = {
        'CNPJ_FUNDO': '51114682000102',
        'INDICE_NPL_DECIMAL': npl_float,
        'OUTRO_ROMPIDO': 3.15e-10 # Teste extra
    }
    df = pd.DataFrame([data])
    
    print(f"\n2. DataFrame Inicial:")
    print(df['INDICE_NPL_DECIMAL'].values[0])
    
    # 3. Simular convert_numeric_columns
    print(f"\n3. Aplicando convert_numeric_columns...")
    cols = ['INDICE_NPL_DECIMAL']
    
    # Debug interno do que data_cleaner faz:
    val_str = str(npl_float)
    print(f"   [Debug] str(float): {val_str}")
    
    df_conv = convert_numeric_columns(df, cols)
    val_conv = df_conv['INDICE_NPL_DECIMAL'].values[0]
    
    print(f"   Valor após conversão: {val_conv}")
    
    if val_conv > 1.0:
        print("   >>> ERRO REPRODUZIDO NA CONVERSÃO! <<<")
    else:
        print("   Conversão OK.")

    # 4. Simular exportação CSV
    print(f"\n4. Simulando Exportação CSV (decimal=',', sep=';', float_format='%.2f')")
    try:
        csv_out = df_conv.to_csv(sep=';', decimal=',', float_format='%.2f', index=False)
        print("   CSV Output (%.2f):")
        print(csv_out.strip())
    except Exception as e:
        print(f"   Erro na exportação 1: {e}")
        
    print(f"\n4.1. Simulando Exportação CSV (SEM float_format)")
    try:
        # Padrão do projeto usa decimal=','
        csv_out_raw = df_conv.to_csv(sep=';', decimal=',', index=False)
        print("   CSV Output (raw com decimal=','):")
        print(csv_out_raw.strip())
        
        # Analisar o valor raw gerado
        lines = csv_out_raw.strip().split('\n')
        if len(lines) > 1:
            val_csv = lines[1].split(';')[1]
            print(f"   Valor no CSV Raw: {val_csv}")
    except Exception as e:
        print(f"   Erro na exportação 2: {e}")

if __name__ == "__main__":
    teste_reproducao()
