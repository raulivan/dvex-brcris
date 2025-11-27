import os
from typing import List
import settings
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st

def __validate_xml(xml_file):
    """Valida se o arquivo gerado está dentro da estrutura definida no modelo

    Args:
        xml_file (String): Caminho do arquivo a ser validado
        model_structure (Dict): Dicionario com a estrutura de validação do modelo
    """

    tree = ET.parse(xml_file)
    root = tree.getroot()
    entity_type_col = []
    file_path_col = []
    message_col = []
    for entity in root.find('entities'):
        achou_um =False
        entity_type = entity.get('type')
        entity_ref = entity.get('ref')
        for field in entity.findall('semanticIdentifier'):
            achou_um = True
        
        if achou_um == False:
            entity_type_col.append(entity_type)
            file_path_col.append(xml_file)
            message_col.append(f"Entidade3 '{entity_ref}' não possui identificador semantico.")
                
    return {
        'entity_type': entity_type_col, 
        'message': message_col,
        'file_path': file_path_col
    }

def rule_07(files_path: List)-> pd.DataFrame:
    """Verifica se todas as entidades tem pelo menos um identificador semantico

    Returns:
        pd.DataFrame: As inconsistências encontradas
    """
    
    st.subheader(f"🧪 Validar a estrutura dos arquivos")    
    status_message = st.empty()              
    
    df = pd.DataFrame(columns=['entity_type', 'message', 'file_path'])
    
    with st.spinner("Verificando se possuem identificadores semânticos..."):
        for xml_file in files_path:
            try:
                vs = __validate_xml(xml_file)
                if len(vs['entity_type']) > 0:
                    new_record_df = pd.DataFrame(vs)
                    df = pd.concat([df, new_record_df], ignore_index=True)
            except Exception as ex:
                temp = {
                    'entity_type': ['Erro'], 
                    'message': [ex],
                    'file_path': xml_file
                }  
                new_record_df = pd.DataFrame(temp)
                df = pd.concat([df, new_record_df], ignore_index=True)   
    if df.empty == True:
        status_message.success(f"✅ Todos os arquivos possuem pelo menos um identificador semânticos")
    else:
        status_message.warning(f"⚠️ Alguns arquivos não possuem indentificador semântico")    
    return df        


