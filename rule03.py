import streamlit as st
from typing import List
import xml.etree.ElementTree as ET
import pandas as pd

def rule_03(files_path: List, model_structure:dict, model_relation:dict)-> pd.DataFrame:
    """Verifica o quantitativo dos conjuntos de dados
    Returns:
        pd.DataFrame1, pd.DataFrame1: Quantitativos de entidades, quantitativos de relacionamentos
    """
    st.subheader(f"🧪 Validar a quantidade de entidades e relacionamentos")            
    
    quantitative_entities = {'xmlReadingError':0}
    for chave in model_structure.keys():
        quantitative_entities[chave] = 0
        
    quantitative_relations = {'xmlReadingError':0}
    for chave in model_relation.keys():
        quantitative_relations[chave] = 0
    
    type = []
    total = []
    
    type_relations = []
    total_relations = []
    
    type.append('Total XML files')
    total.append(len(files_path))
    with st.spinner("Contabilizando as entidades e relacionamentos presentes no arquivos..."):
        for xml_file in files_path:
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                for entity in root.find('entities'):
                    entity_type = entity.get('type')
                    quantitative_entities[entity_type] = quantitative_entities[entity_type] + 1
                    
                for relation in root.find('relations'):
                    relation_type = relation.get('type')
                    quantitative_relations[relation_type] = quantitative_relations[relation_type] + 1
                
            except Exception as ex:
                quantitative_entities['xmlReadingError'] = quantitative_entities['xmlReadingError'] + 1
                quantitative_relations['xmlReadingError'] = quantitative_relations['xmlReadingError'] + 1
                
    for chave in model_structure.keys():
        if quantitative_entities[chave] > 0:
            type.append(chave)
            total.append(quantitative_entities[chave])
    
    for chave in model_relation.keys():
        if quantitative_relations[chave] > 0:
            type_relations.append(chave)
            total_relations.append(quantitative_relations[chave])
        
        
    dicionario = {
                'type': type, 
                'total': total
            }  
    
    dicionario_relations = {
                'type': type_relations, 
                'total': total_relations
            }  
    return  pd.DataFrame(dicionario), pd.DataFrame(dicionario_relations)        


