import os
from typing import List
import settings
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
from ulti import load_model_structure, load_model_relations, read_csv

def rule_04(files_path: List)-> pd.DataFrame:
    """Verifica se as organições existentes são validas
    Returns:
        pd.DataFrame1, pd.DataFrame1: Quantitativos de orgunist, Listagem de arquivos inválidos
    """
    st.subheader(f"🧪 Validar OrgUnits")          
    
    dict_orgunit = {}
    allowed_orgunits = read_csv(path=settings.ALLOWED_ORGUNITS_PATH)
    for item in allowed_orgunits:
        dict_orgunit[item[0]] = item[0]
    
    erros = 0
    quantitative_orgunits = 0
    quantitative_orgunits_valid = 0
    quantitative_orgunits_invalid = 0
    files_invalid = []
    

    with st.spinner("Verificando as OrgUnits informadas nos arquivos..."):
        for xml_file in files_path:
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                for entity in root.find('entities'):
                    entity_type = entity.get('type')
                    if entity_type == 'OrgUnit' or entity_type == 'Orgunit' or entity_type.lower() == 'orgunit':
                        quantitative_orgunits = quantitative_orgunits + 1
                        name_orgunit = ''
                        for field in entity.findall('field'):
                            field_name = field.get('name')
                            if field_name == 'name':
                                name_orgunit = field.get('value')
                                
                        
                        if name_orgunit in dict_orgunit:
                            quantitative_orgunits_valid = quantitative_orgunits_valid + 1
                        else:
                            quantitative_orgunits_invalid = quantitative_orgunits_invalid + 1
                            files_invalid.append(xml_file)
            except Exception as ex:
                erros = erros + 1
                files_invalid.append(xml_file)
                
    
        
    df_quantitativos_orgunit = {
                'type': ['Total Orgunits','Orgunits Valid', 'Orgunits invalid','Erros'], 
                'total': [quantitative_orgunits,quantitative_orgunits_valid, quantitative_orgunits_invalid, erros]
            }  
    
    dicionario_relations = {
                'files': files_invalid
            }  
    return  pd.DataFrame(df_quantitativos_orgunit), pd.DataFrame(dicionario_relations)        


