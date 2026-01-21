import sys
import os
sys.path.append(os.getcwd())
import logging
import pandas as pd
from src.services.etl_service import FIDCETLService
from src.transformers.data_cleaner import convert_numeric_columns

# Silenciar logs excessivos
logging.getLogger().setLevel(logging.WARNING)

def verify():
    cnpj = "51114682000102"
    print(f"=== VERIFICAÇÃO END-TO-END: CNPJ {cnpj} ===")
    
    service = FIDCETLService()
    
    # 1. Obter dados reais da API B3 -> XML Parser
    print("1. Buscando e parseando dados (Pipeline Real)...")
    try:
        raw_data = service.process_single_cnpj(cnpj)
    except Exception as e:
        print(f"FALHA na busca: {e}")
        return

    if raw_data.get('STATUS') == 'SUCESSO':
        print("   Dados extraídos com sucesso.")
    else:
        print(f"   Erro na extração: {raw_data.get('STATUS')} - {raw_data.get('MENSAGEM_ERRO')}")
        return
        
    # 2. Verificar valor bruto no dicionário (antes de qualquer DF/cleaner)
    original_npl = raw_data.get('INDICE_NPL_DECIMAL')
    print(f"\n2. Valor NPL vindo do XMLParser: {original_npl}")
    print(f"   Tipo: {type(original_npl)}")
    
    # 3. Simular pipeline de transformação (FIDCETLService.process_and_validate)
    print("\n3. Aplicando pipeline de transformação (DataFrame + Cleaner)...")
    df = pd.DataFrame([raw_data])
    
    cols_numericas = [
        'INDICE_NPL_DECIMAL', 'CARTEIRA_BRUTA', 'INADIMPLENCIA_TOTAL'
    ]
    
    # Aplicar a função corrigida
    df_clean = convert_numeric_columns(df, cols_numericas)
    
    val_final = df_clean['INDICE_NPL_DECIMAL'].values[0]
    print(f"   Valor NPL Final (Pós-Limpeza): {val_final}")
    
    # 4. Validação
    expected_threshold = 0.01 # 1%
    
    if val_final < expected_threshold:
        print("\n✅ SUCESSO: O NPL manteve a magnitude correta (decimal pequeno).")
        print(f"   Valor final: {val_final:.15f}")
    else:
        print(f"\n❌ FALHA: O NPL explodiu para {val_final}, o que é maior que 100%.")

if __name__ == "__main__":
    verify()
