import os
import uuid
from sqlite3 import Connection
from typing import List
import settings
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
from ulti import load_model_structure, load_model_relations, read_csv, connect_local_database, execute_sql


def __create_database(db:Connection):

    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_semantic_identifier ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    identifier text, 
                    entity_id text,
                    file text
                ); 
                """)
    
    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_entity_fields ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    entity_id text,
                    type VARCHAR(100),
                    name text,
                    value text,
                    file text
                ); 
                """)

    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_entity_relations ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    de_entity_id text,
                    para_entity_id text,
                    type text,
                    file text                    
                ); 
                """)
    
    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_semantic_identifier_deduplicated ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    identifier text, 
                    entity_id text
                ); 
                """)
    
    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_entity_fields_deduplicated ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    entity_id text,
                    type VARCHAR(100),
                    name text,
                    value text
                ); 
                """)

    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_entity_relations_deduplicated ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    de_entity_id text,
                    para_entity_id text,
                    type text                    
                ); 
                """)

    db.commit()

def rule_05(files_path: List)-> pd.DataFrame:
    
    st.subheader(f"🧪 Simulando caraga e deduplicação")    
    status_message = st.empty()   
    
    if os.path.exists(settings.LOCAL_DATABASE_PATH):
        os.remove(settings.LOCAL_DATABASE_PATH)

    db = connect_local_database()
    __create_database(db)
    
    with st.spinner("Realizando a carga de dados..."):
        contador = 0
        for xml_file in files_path:
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                de_para_entity = {}
                
                tb_semantic_identifier = []
                tb_entity_fields = {}
                tb_entity_relations = {}
                
                # Percorrendo cada entidade do arquivo
                for entity in root.find('entities'):
                    entity_type = entity.get('type')
                    entity_ref = entity.get('ref')
                    entity_id = uuid.uuid4()
                    
                    de_para_entity[entity_ref]={
                        'id': entity_id.hex,
                        'type': entity_type,
                        'file': xml_file
                    }
                    
                    # Recuperando os identificadores semanticos
                    for semantic_id  in entity.findall('.//semanticIdentifier'):                            
                        tb_semantic_identifier.append((entity_ref, semantic_id.text))
                            
                    # Recuperando os campos da entidade
                    for field in entity.findall('field'):
                        field_name = field.get('name')
                        field_value = field.get('value')
                        if not entity_ref in tb_entity_fields:
                            tb_entity_fields[entity_ref] = [(field_name, field_value)]
                        else:
                            tb_entity_fields[entity_ref].append((field_name, field_value))
                
                # Recuperando os relacionamentos
                for relation in root.find('relations'):
                    relation_type = relation.get('type')
                    fromEntityRef = relation.get('fromEntityRef')
                    toEntityRef = relation.get('toEntityRef')
                    
                    if not relation_type in tb_entity_relations:
                        tb_entity_relations[relation_type] = [(fromEntityRef, toEntityRef)]
                    else:
                        tb_entity_relations[relation_type].append((fromEntityRef, toEntityRef))
                
                # Inserindo no banco de dados os identificadores semanticos
                for item in tb_semantic_identifier:
                    entity_ref = item[0] 
                    identifier = item[1]
                    entity_id = de_para_entity[entity_ref]['id']       
                    file = de_para_entity[entity_ref]['file']                               
                    sql_insert = f"INSERT INTO tb_semantic_identifier (identifier, entity_id, file) VALUES ('{identifier}', '{entity_id}','{file}');"
                    execute_sql(conn=db,sql=sql_insert)
                    contador = contador + 1
                    if contador > 10000:
                        db.commit()
                        contador = 0
                    
                for chave, valor in tb_entity_fields.items():
                    entity_id = de_para_entity[chave]['id'] 
                    entity_type = de_para_entity[chave]['type'] 
                    file = de_para_entity[entity_ref]['file']      
                    
                    for item in valor:
                        name = item[0]
                        value = item[1]
                        sql_insert = f"INSERT INTO tb_entity_fields (entity_id, type, name, value, file) VALUES ('{entity_id}', '{entity_type}', '{name}', '{value}','{file}');"
                        execute_sql(conn=db,sql=sql_insert)
                        contador = contador + 1
                        if contador > 10000:
                            db.commit()
                            contador = 0
                
                for chave, valor in tb_entity_relations.items():
                    relation_type = chave
                    
                    for item in valor:
                        de_entity_id = de_para_entity[item[0]]['id'] 
                        para_entity_id = de_para_entity[item[1]]['id'] 
                        file = de_para_entity[entity_ref]['file']   
                        sql_insert = f"INSERT INTO tb_entity_relations (de_entity_id, para_entity_id, type, file) VALUES ('{de_entity_id}', '{para_entity_id}', '{relation_type}','{file}');"
                        execute_sql(conn=db,sql=sql_insert)
                        contador = contador + 1
                        if contador > 10000:
                            db.commit()
                            contador = 0
                        
            except Exception as ex:
                status_message.error(f"❌ {ex}")   
    
    db.commit()
    status_message.success(f"✅ Processo concluído com sucesso")
    
    