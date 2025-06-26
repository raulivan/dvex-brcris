import os
from typing import List
import settings
import uuid
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm
from ulti import load_model_structure, load_model_relations

def rule_03xxx(files_path: List)-> pd.DataFrame:
    """Verifica a deduplicação dos registros
    Returns:
        pd.DataFrame: As inconsistências encontradas
    """
    print('Verifica dados deduplicados')
    
    model_file = settings.XML_BRCRIS_MODEL_PATH

    model_structure = load_model_structure(model_file)
    
    quantitative_entities_deduplicate = {}
    for chave in model_structure.keys():
        quantitative_entities_deduplicate[chave] = 0
        
    entidade_unica = {}
    de_para = {}
    for chave in model_structure.keys():
        de_para[chave] = {}
        entidade_unica[chave] = {}

    with tqdm(total=len(files_path)) as progress:
        for xml_file in files_path:
            progress.update(1)
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                # Percorrendo todas as entidades do arquivo
                for entity in root.find('entities'):
                    entity_type = entity.get('type')
                    list_si = []
                    # Percorrendo todos os identificadores semanticos da entidade
                    for semantic_id  in entity.findall('.//semanticIdentifier'):
                        list_si.append(semantic_id .text)
                    
                    # Verificando se esse identificador ja foi usado anterioremente
                    uuid_entity = None
                    new_entity = True
                    for si_currente in list_si:
                        if si_currente in de_para[entity_type]:
                            new_entity = False
                            break
                    
                    # Não estava na lista então sera inserido
                    if new_entity == True:
                        uuid_entity = uuid.uuid4()
                        for si_currente in list_si:
                            de_para[entity_type][si_currente]=uuid_entity.hex 
                        
                        entidade_unica[entity_type][uuid_entity.hex]=uuid_entity.hex
                        
            except Exception as ex:
                continue
    type = []
    total = []            
    for chave in model_structure.keys():
        if len(entidade_unica[chave].keys()) > 0:
            type.append(chave)
            total.append(len(entidade_unica[chave].keys()))
        
        
    dicionario = {
                'type': type, 
                'total': total
            }  
    
    return  pd.DataFrame(dicionario)


