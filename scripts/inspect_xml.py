"""
Script para baixar e inspecionar XML de CNPJ problemático.

Investiga o formato real dos campos NPL no XML da B3.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extractors.api_client import B3ApiClient
import xml.etree.ElementTree as ET

def inspecionar_xml(cnpj: str):
    """Baixa e inspeciona XML de um CNPJ."""
    
    print(f"=== INVESTIGANDO CNPJ: {cnpj} ===\n")
    
    # 1. Buscar documentos
    client = B3ApiClient()
    sucesso, df_docs, erro = client.buscar_documentos(cnpj)
    
    if not sucesso:
        print(f"❌ Erro ao buscar: {erro}")
        return
    
    print(f"✅ {len(df_docs)} documentos encontrados")
    
    # 2. Filtrar informe mensal
    sucesso, df_mensal, erro = client.filtrar_informe_mensal(df_docs)
    
    if not sucesso:
        print(f"❌ Erro ao filtrar: {erro}")
        return
    
    print(f"✅ {len(df_mensal)} informes mensais encontrados")
    
    # 3. Pegar documento mais recente
    sucesso, doc, erro = client.selecionar_documento_mais_recente(df_mensal)
    
    if not sucesso:
        print(f"❌ Erro ao selecionar: {erro}")
        return
    
    print(f"✅ Documento selecionado: {doc.get('dataReferencia', 'N/A')}")
    
    # 4. Baixar XML
    sucesso, xml_content, erro = client.baixar_xml(doc['id'])
    
    if not sucesso:
        print(f"❌ Erro ao baixar: {erro}")
        return
    
    print(f"✅ XML baixado: {len(xml_content)} bytes\n")
    
    # 5. Parsear e inspecionar
    try:
        root = ET.fromstring(xml_content)
        print("=== CAMPOS RELEVANTES DO XML ===\n")
        
        # Função helper para buscar e exibir
        def exibir_campo(nome, caminho):
            elem = root.find(f'.//{caminho}')
            if elem is not None and elem.text:
                print(f"{nome}:")
                print(f"  Caminho: {caminho}")
                print(f"  Valor RAW: '{elem.text}'")
                print(f"  Tipo: {type(elem.text)}")
                print()
        
        # Campos básicos
        exibir_campo("CNPJ Fundo", "NR_CNPJ_FUNDO")
        exibir_campo("Data Competência", "DT_COMPT")
        
        # Campos de crédito
        exibir_campo("Créditos Inadimplência", "CRED_EXISTE/VL_CRED_EXISTE_INAD")
        exibir_campo("Créditos Adquiridos", "CRED_EXISTE/VL_SOM_DICRED_AQUIS")
        exibir_campo("Direitos Creditórios Total", "DICRED/VL_DICRED")
        exibir_campo("Direitos Creditórios Inadimplência", "DICRED/VL_DICRED_EXISTE_INAD")
        
        # Campos de ativo
        exibir_campo("Ativo Total", "VL_SOM_APLIC_ATIVO")
        exibir_campo("Carteira Total", "VL_CARTEIRA")
        
        # CAMPO CRÍTICO: Procurar todos os campos que comecem com "INDICE" ou "NPL" ou "INAD"
        print("=== CAMPOS CONTENDO 'INDICE', 'NPL', 'INAD', 'TAXA' ===\n")
        
        for elem in root.iter():
            tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            
            if any(keyword in tag.upper() for keyword in ['INDICE', 'NPL', 'INAD', 'TAXA', 'PERCENT']):
                if elem.text and elem.text.strip():
                    print(f"Tag: {tag}")
                    print(f"  Valor: '{elem.text}'")
                    print()
        
        # Salvar XML para inspeção manual
        output_file = Path(__file__).parent.parent / "outputs" / f"xml_debug_{cnpj}.xml"
        with open(output_file, 'wb') as f:
            f.write(xml_content)
        
        print(f"\n✅ XML salvo em: {output_file}")
        
    except Exception as e:
        print(f"❌ Erro ao parsear XML: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Investigar os 2 CNPJs problemáticos
    cnpjs_problema = [
        "51114682000102",  # NPL = 31 trilhões
        "21397715000108",  # NPL = 4 milhões
    ]
    
    for cnpj in cnpjs_problema:
        inspecionar_xml(cnpj)
        print("\n" + "="*80 + "\n")
