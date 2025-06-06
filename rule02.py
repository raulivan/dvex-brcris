import os
from typing import List
import settings
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st

def __validate_xml(xml_file, model_structure):
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
        entity_type = entity.get('type')
        if entity_type not in model_structure:
            entity_type_col.append(entity_type)
            file_path_col.append(xml_file)
            message_col.append('Model not defined')
            continue
        
        model_fields = model_structure[entity_type]
        for field in entity.findall('field'):
            field_name = field.get('name')
            if field_name not in model_fields:
                entity_type_col.append(entity_type)
                file_path_col.append(xml_file)
                message_col.append(f"O Campo '{field_name}' na entidade '{entity_type}' não está definida no modelo.")
                
    return {
        'entity_type': entity_type_col, 
        'message': message_col,
        'file_path': file_path_col
    }

def rule_02(files_path: List, model_structure: dict)-> pd.DataFrame:
    """Verifica se o conjunto de dados esta dentro do Modelo definido

    Returns:
        pd.DataFrame: As inconsistências encontradas
    """
    
    st.subheader(f"🧪 Validar a estrutura dos arquivos")    
    status_message = st.empty()              
    
    df = pd.DataFrame(columns=['entity_type', 'message', 'file_path'])
    
    with st.spinner("Verificando a estrutuda dos arquivos..."):
        for xml_file in files_path:
            try:
                vs = __validate_xml(xml_file, model_structure)
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
        status_message.success(f"✅ Todos os arquivos estão em conformidade com o modelo informado")
    else:
        status_message.warning(f"⚠️ Alguns arquivos apresentaram inconformidade com o modelo informado")    
    return df        


