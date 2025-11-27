import os
import uuid
from sqlite3 import Connection
from typing import List
import settings
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
from ulti import connect_deduplicated_database, format_text_sql_field, connect_local_database, execute_sql


def __create_database(db:Connection):

    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_semantic_identifier ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    identifier text, 
                    entity_id text,
                    file text,
                    type VARCHAR(100)
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
   


    execute_sql(conn=db,sql="CREATE INDEX idx_tb_semantic_identifier_identifier ON tb_semantic_identifier (identifier);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_semantic_identifier_entity_id ON tb_semantic_identifier (entity_id);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_semantic_identifier_type ON tb_semantic_identifier (type);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_fields_entity_id ON tb_entity_fields (entity_id);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_fields_type ON tb_entity_fields (type);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_fields_name ON tb_entity_fields (name);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_relations_de_entity_id ON tb_entity_relations (de_entity_id);")

    
    
    execute_sql(conn=db,sql="""
                CREATE VIEW IF NOT EXISTS vw_entidades
                AS
                SELECT distinct entity_id,type 
                FROM tb_entity_fields
                """)
    
    execute_sql(conn=db,sql="""
               CREATE VIEW IF NOT EXISTS vw_semantic_identifier_duplicado
                AS 		
                    SELECT 
                        tb_semantic_identifier.type as entity_type,
                        tb_semantic_identifier.entity_id,
                        substr(tb_semantic_identifier.identifier, 1, instr(tb_semantic_identifier.identifier, '::') - 1) AS namespace,
                        count(tb_semantic_identifier.id) as total
                    FROM 
                        tb_semantic_identifier
                    GROUP BY 
                        tb_semantic_identifier.type,
                        tb_semantic_identifier.entity_id,
                        substr(tb_semantic_identifier.identifier, 1, instr(tb_semantic_identifier.identifier, '::') - 1)
                    having count(tb_semantic_identifier.id) > 1
                    ORDER BY
                        substr(tb_semantic_identifier.identifier, 1, instr(tb_semantic_identifier.identifier, '::') - 1)
                    
                """)
    
    db.commit()
    
def __create_deduplicated_database(db:Connection):

    
    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_semantic_identifier_deduplicated ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    identifier text, 
                    entity_id text,
                    type VARCHAR(100)
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
        
    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_de_para ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    entity_id_de text, 
                    entity_id_para text,
                    type text     
                ); 
                """)
    
    execute_sql(conn=db,
                sql="""
                CREATE TABLE tb_entity_files ( 
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                    entity_id text,
                    file text                    
                ); 
                """)
   
    
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_de_para_entity_id_de ON tb_de_para (entity_id_de);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_de_para_entity_id_para ON tb_de_para (entity_id_para);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_semantic_identifier_deduplicated_identifier ON tb_semantic_identifier_deduplicated (identifier);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_semantic_identifier_deduplicated_entity_id ON tb_semantic_identifier_deduplicated (entity_id);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_semantic_identifier_deduplicated_type ON tb_semantic_identifier_deduplicated (type);")

    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_fields_deduplicated_entity_id ON tb_entity_fields_deduplicated (entity_id);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_fields_deduplicated_type ON tb_entity_fields_deduplicated (type);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_fields_deduplicated_name ON tb_entity_fields_deduplicated (name);")

    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_relations_deduplicated_de_entity_id ON tb_entity_relations_deduplicated (de_entity_id);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_relations_deduplicated_para_entity_id ON tb_entity_relations_deduplicated (para_entity_id);")
    
    execute_sql(conn=db,sql="CREATE INDEX idx_tb_entity_files_entity_id ON tb_entity_files (entity_id);")
    
    
    execute_sql(conn=db,sql="""
                CREATE VIEW IF NOT EXISTS vw_entidades_deduplicated
                AS
                SELECT distinct entity_id,type 
                FROM tb_entity_fields_deduplicated
                """)

    execute_sql(conn=db,sql="""
               CREATE VIEW IF NOT EXISTS vw_semantic_identifier_duplicado_deduplicated
                AS 		
                    SELECT 
                        tb_semantic_identifier_deduplicated.type as entity_type,
                        tb_semantic_identifier_deduplicated.entity_id,
                        substr(tb_semantic_identifier_deduplicated.identifier, 1, instr(tb_semantic_identifier_deduplicated.identifier, '::') - 1) AS namespace,
                        count(tb_semantic_identifier_deduplicated.id) as total
                    FROM 
                        tb_semantic_identifier_deduplicated
                    GROUP BY 
                        tb_semantic_identifier_deduplicated.type,
                        tb_semantic_identifier_deduplicated.entity_id,
                        substr(tb_semantic_identifier_deduplicated.identifier, 1, instr(tb_semantic_identifier_deduplicated.identifier, '::') - 1)
                    having count(tb_semantic_identifier_deduplicated.id) > 1
                    ORDER BY
                        substr(tb_semantic_identifier_deduplicated.identifier, 1, instr(tb_semantic_identifier_deduplicated.identifier, '::') - 1)
                    
                """)
    
    
    
    db.commit()

def rule_05(files_path: List)-> pd.DataFrame:
    
    st.subheader(f"🧪 Caraga de dados")    
    status_message = st.empty()   
    
    if os.path.exists(settings.LOCAL_DATABASE_PATH):
        os.remove(settings.LOCAL_DATABASE_PATH)

    db = connect_local_database()
    deduplicated_db = connect_deduplicated_database()
    
    __create_database(db)
    __create_deduplicated_database(deduplicated_db)
    conmtador_auxiliar = 0
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
                    conmtador_auxiliar = conmtador_auxiliar + 1
                    
                    entity_type = entity.get('type')
                    entity_ref = entity.get('ref')
                    entity_id = uuid.uuid4()
                    
                    if entity_ref == None:
                        entity_ref = f'{entity_type}{conmtador_auxiliar}'
                    
                    de_para_entity[entity_ref]={
                        'id': entity_id.hex,
                        'type': entity_type,
                        'file': xml_file
                    }
                    
                    # Recuperando os identificadores semanticos
                    for semantic_id  in entity.findall('.//semanticIdentifier'):    
                        semantic_id_value = semantic_id.text  
                        if (semantic_id_value == None) or (semantic_id_value == ''):
                            semantic_id_value =  semantic_id.get('value')
                       
                        tb_semantic_identifier.append((entity_ref, semantic_id_value))
                            
                    # Recuperando os campos da entidade
                    for field in entity.findall('field'):
                        field_name = field.get('name')
                        field_value = format_text_sql_field(field.get('value'))
                        
                       
                        if field_value != 'null':
                            if not entity_ref in tb_entity_fields:
                                tb_entity_fields[entity_ref] = [(field_name, field_value)]
                            else:
                                tb_entity_fields[entity_ref].append((field_name, field_value))
                        else:
                            for sub_field in field.findall('field'):
                                sub_field_name = sub_field.get('name')
                                sub_field_value = format_text_sql_field(sub_field.get('value'))
                                
                                field_name_formated = f"{field_name}.{sub_field_name}"
                                
                                if not entity_ref in tb_entity_fields:
                                    tb_entity_fields[entity_ref] = [(field_name_formated, sub_field_value)]
                                else:
                                    tb_entity_fields[entity_ref].append((field_name_formated, sub_field_value))
                                
                
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
                    type = de_para_entity[entity_ref]['type']                               
                    sql_insert = f"INSERT INTO tb_semantic_identifier (identifier, entity_id, file, type) VALUES ('{identifier}', '{entity_id}','{file}','{type}');"
                    execute_sql(conn=db,sql=sql_insert)
                    contador = contador + 1
                    if contador > settings.LIMIT_COMMIT:
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
                        if contador > settings.LIMIT_COMMIT:
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
                        if contador > settings.LIMIT_COMMIT:
                            db.commit()
                            contador = 0
                        
            except Exception as ex:
                status_message.error(f"❌ {ex}")   
    
    db.commit()
    db.close()
    deduplicated_db.close()
    status_message.success(f"✅ Carga concluída com sucesso")
    
    