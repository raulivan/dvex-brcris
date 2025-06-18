from typing import List
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st

def rule_01(files_path: List)-> pd.DataFrame:
    """Verifica se todos os arquivos possuem a extesão .xml em minusculo

    Returns:
        pd.DataFrame: As inconsistências encontradas
    """
    
    st.subheader(f"🧪 Validar extensão de arquivos")
    status_message = st.empty()
    erros = 0                
    
    df = pd.DataFrame(columns=['entity_type', 'message', 'file_path'])
    
    with st.spinner("Os arquivos possuem um nome válido..."):
        for xml_file in files_path:
            try:
                if str(xml_file).endswith(".xml") == False:
                    erros = erros + 1
                    temp = {
                        'entity_type': ['Extensão inválida'], 
                        'message': ['a extensão do arquivo deve ser “.xml” em minúsculo'],
                        'file_path': [xml_file]
                    }  
                    new_record_df = pd.DataFrame(temp)
                    df = pd.concat([df, new_record_df], ignore_index=True)  
                    
            except Exception as ex:
                erros = erros + 1
                temp = {
                    'entity_type': ['Erro'], 
                    'message': [ex],
                    'file_path': xml_file
                }  
                new_record_df = pd.DataFrame(temp)
                df = pd.concat([df, new_record_df], ignore_index=True)   
    
    if erros == 0:
        status_message.success(f"✅ Todos os arquivos possuem a extensão correta")
    elif erros == len(files_path):
        status_message.error(f"❌ Todos os arquivos possuem a extensão incorreta") 
    else:
        status_message.warning(f"⚠️ {erros} arquivo(s) possui(em) a extensão incorreta")     
                
    return df        


