import os
import uuid
from sqlite3 import Connection
from typing import List
import settings
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
from ulti import get_result_rows, connect_local_database, execute_sql, get_scalar


def rule_06(files_path: List)-> pd.DataFrame:
    
    st.subheader(f"🧪 Deduplicação de dados")    
    status_message = st.empty()   
    
    db = connect_local_database()
    
    de_para_entity = {}
    with st.spinner("Executando o processo de deduplicação dos dados..."):
        contador = 0
        # INCLUIR O TYPE E TESTAR NOVAMENTE
        list_semantic_identifier = get_result_rows(conn=db,sql="SELECT DISTINCT identifier, type FROM tb_semantic_identifier")
        for semantic_identifier_current in list_semantic_identifier:
            try:
                semantic_identifier = semantic_identifier_current[0]
                type = semantic_identifier_current[1]
                
                list_entity_id = []
                entity_id_master = None
                list_entities = get_result_rows(conn=db,sql=f"SELECT DISTINCT entity_id FROM tb_semantic_identifier WHERE identifier = '{semantic_identifier}' and type = '{type}'")
                
                # Definindo a entidade principal de destino
                for entity in list_entities:
                    if entity_id_master == None:
                        entity_id_master = entity[0]
                    
                    list_entity_id.append(entity[0])
                    de_para_entity[entity[0]]= entity_id_master
                
                # Recuperando os dados

                # Colocar cada item entre aspas simples
                list_entity_id_com_aspas = [f"'{item}'" for item in list_entity_id]
                entity_id_para_in = ', '.join(list_entity_id_com_aspas)
                
                # Pegando os identificadores semanticos
                list_unico_semantic_identifier = get_result_rows(conn=db,sql=f"SELECT DISTINCT identifier FROM tb_semantic_identifier where entity_id in ({entity_id_para_in})")
                for item in list_unico_semantic_identifier:
                    item_identifier = item[0]
                    
                    sql_insert = f"INSERT INTO tb_semantic_identifier_deduplicated (identifier, entity_id, type) VALUES ('{item_identifier}', '{entity_id_master}', '{type}');"
                    execute_sql(conn=db,sql=sql_insert)
                    contador = contador + 1
                    if contador > settings.LIMIT_COMMIT:
                        db.commit()
                        contador = 0
                
                # Pegando os campos
                list_unico_entity_fields = get_result_rows(conn=db,sql=f"SELECT DISTINCT type,name,value FROM tb_entity_fields where entity_id in ({entity_id_para_in})")
                for item in list_unico_entity_fields:
                    item_type = item[0]
                    item_name = item[1]
                    item_value = item[2]
                    
                    sql_insert = f"INSERT INTO tb_entity_fields_deduplicated (entity_id, type, name, value) VALUES ('{entity_id_master}', '{item_type}', '{item_name}', '{item_value}');"
                    execute_sql(conn=db,sql=sql_insert)
                    contador = contador + 1
                    if contador > settings.LIMIT_COMMIT:
                        db.commit()
                        contador = 0

            except Exception as ex:
                status_message.error(f"❌ {ex}")   
        
        # Agora que já deduplicou tudo, corrigir os relacionamentos
        list_entity_relations = get_result_rows(conn=db,sql="SELECT DISTINCT de_entity_id, para_entity_id,type  FROM tb_entity_relations")
        ja_realizado = {}
        for entity_relations_current in list_entity_relations:
            try:
                de_entity_id_old = entity_relations_current[0]
                para_entity_id_old = entity_relations_current[1]
                type = entity_relations_current[2]
                
                de_entity_id_new = de_para_entity[de_entity_id_old]
                para_entity_id_new = de_para_entity[para_entity_id_old]
                
                if de_entity_id_new in ja_realizado:
                    if ja_realizado[de_entity_id_new] == para_entity_id_new:
                        continue
                
                ja_realizado[de_entity_id_new] = para_entity_id_new
                
                sql_insert = f"INSERT INTO tb_entity_relations_deduplicated (de_entity_id, para_entity_id, type) VALUES ('{de_entity_id_new}', '{para_entity_id_new}', '{type}');"
                execute_sql(conn=db,sql=sql_insert)
                contador = contador + 1
                if contador > settings.LIMIT_COMMIT:
                    db.commit()
                    contador = 0
            except Exception as ex:
                    status_message.error(f"❌ {ex}")  
        # Armazena o De Para
        for chave, valor in de_para_entity.items():
            de_entity_id = chave
            para_entity_id = valor
            
            # Pega o arquivo de origem do De
            file = str(get_scalar(conn=db,sql=f"SELECT DISTINCT file FROM tb_semantic_identifier WHERE entity_id = '{de_entity_id}'"))
              
            sql_insert = f"INSERT INTO tb_de_para (entity_id_de, entity_id_para, file) VALUES ('{de_entity_id}', '{para_entity_id}', '{file}');"
            execute_sql(conn=db,sql=sql_insert)
            contador = contador + 1
            if contador > settings.LIMIT_COMMIT:
                db.commit()
                contador = 0   
    
    db.commit()
    
    # Contabilizando os elementos
    l_type = []
    l_antes = []
    l_depois = []
    
    antes = get_result_rows(conn=db,sql="""
                                            SELECT TYPE,COUNT( DISTINCT entity_id) AS TOTAL
                                            FROM tb_entity_fields
                                            GROUP BY TYPE
                                            ORDER BY TYPE
                                            """)
    for item in antes:
        l_type.append(item[0])
        l_antes.append(item[1])
        
    
    depois = get_result_rows(conn=db,sql="""
                                        SELECT TYPE,COUNT( DISTINCT entity_id) AS TOTAL
                                        FROM tb_entity_fields_deduplicated
                                        GROUP BY TYPE
                                        ORDER BY TYPE
                                        """)    
    for item in depois:
        l_depois.append(item[1])
        
    dicionario = {
                'type': l_type, 
                'antes': l_antes,
                'depois': l_depois
            } 
    
    status_message.success(f"✅ Carga concluída com sucesso")
    
    return  pd.DataFrame(dicionario)
    
    