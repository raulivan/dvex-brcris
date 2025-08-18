import os
import uuid
from sqlite3 import Connection
from typing import List
import settings
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

from ulti import connect_deduplicated_database, get_result_rows, connect_local_database, execute_sql, get_scalar


def rule_06(files_path: List)-> pd.DataFrame:
    
    st.subheader(f"🧪 Deduplicação de dados")    
    status_message = st.empty()   
    
    db = connect_local_database()
    deduplicated_db = connect_deduplicated_database()
    
    
    with st.spinner("Deduplicando as entidades..."):
        status_message.error(f"Construindo o grafo...")   
        contador = 0
        # Carrega o conjunto de dados
        df = pd.read_sql(con=db, sql="SELECT DISTINCT ('[' || type || ']' || identifier) AS identifier, entity_id,type, file FROM tb_semantic_identifier")
        
        # Construindo o grafo de equivalência
        G = nx.Graph()
        
        # Adiciona os nós e arestas
        for _, row in df.iterrows():
            identifier = row['identifier']
            entity = row['entity_id']
            
            # Adiciona nós
            G.add_node(identifier, type='identifier', color='lightblue')
            G.add_node(entity, type='entity_id', color='lightgreen')
            
            # Adiciona aresta
            G.add_edge(identifier, entity)

        # Gerar e salvar visualização do grafo
        # plt.figure(figsize=(12, 8))
        # pos = nx.spring_layout(G)
        
        # Cores customizadas por tipo
        # colors = [G.nodes[node]['color'] for node in G.nodes()]
        
        # nx.draw_networkx(
        #     G, 
        #     pos,
        #     with_labels=True,
        #     node_size=1500,
        #     node_color=colors,
        #     font_size=8,
        #     edge_color='gray'
        # )
        
        # plt.title("Grafo de Deduplicação de Entidades", size=15)
        # plt.axis('off')
        # plt.tight_layout()
        # plt.savefig('grafo_deduplicacao.png', dpi=300)
        # plt.close()

        # 5. Processar deduplicação (continua igual)
        status_message.error(f"Recuperando componentes conectados...")  
        components = list(nx.connected_components(G))
        
        id_to_group = {}
        for group in components:
            # Encontra o primeiro identificador no grupo ou o primeiro nó
            representative = next((x for x in group if G.nodes[x]['type'] == 'identifier'), min(group))
            for id in group:
                id_to_group[id] = representative
        
        df['group_id'] = df['identifier'].map(id_to_group)
        
        result = df.groupby('group_id').agg({
            'type': 'first',
            'identifier': lambda x: '|'.join(x),
            'entity_id': lambda x: '|'.join(x),
            'file': lambda x: '|'.join(x)
        }).reset_index(drop=True)
        
        
        status_message.error(f"Salvando as entidades...")  
        for _, row in result.iterrows():
            
            tipo = row['type']
            identifier = row['identifier']
            entity = row['entity_id']
            file = row['file']
            
            entity_list = str(entity).split('|')
            entity_id_master = entity_list[0]
            
            file_list = str(file).split('|')
            
            for item in entity_list:
                sql_insert = f"INSERT INTO tb_de_para (entity_id_de, entity_id_para, type) VALUES ('{item}', '{entity_id_master}','{tipo}');"
                execute_sql(conn=deduplicated_db,sql=sql_insert)
                contador = contador + 1
            
            for item in file_list:
                sql_insert = f"INSERT INTO tb_entity_files (entity_id, file) VALUES ('{entity_id_master}', '{item}');"
                execute_sql(conn=deduplicated_db,sql=sql_insert)
                contador = contador + 1
            
            
            if contador > settings.LIMIT_COMMIT:
                db.commit()
                deduplicated_db.commit()
                contador = 0
        
        
        deduplicated_db.commit()
        contador = 0
        
        list_entity_de_para_id_to_entity = get_result_rows(conn=deduplicated_db,sql="SELECT DISTINCT entity_id_para, type FROM tb_de_para")         
        
        #  percorre todas as entidades masters para recuperar todos os Identificadores semanticos
        for entity_id_current in list_entity_de_para_id_to_entity:
            try:
                entity_id_master = entity_id_current[0]
                type_master = entity_id_current[1]
                
                # Pegando as mesma entidades
                list_entidades = get_result_rows(conn=deduplicated_db,sql=f"SELECT DISTINCT entity_id_de FROM tb_de_para where entity_id_para = '{entity_id_master}'")
                list_entidades_com_aspas = [f"'{item[0]}'" for item in list_entidades]
                list_entidades_com_aspas_in = ', '.join(list_entidades_com_aspas)
                
                # Pegando os identificadores semanticos
                list_unico_semantic_identifier = get_result_rows(conn=db,sql=f"SELECT DISTINCT identifier FROM tb_semantic_identifier where entity_id in ({list_entidades_com_aspas_in})")
                for item in list_unico_semantic_identifier:
                    item_identifier = item[0]
                    
                    sql_insert = f"INSERT INTO tb_semantic_identifier_deduplicated (identifier, entity_id, type) VALUES ('{item_identifier}', '{entity_id_master}', '{type_master}');"
                    execute_sql(conn=deduplicated_db,sql=sql_insert)
                    contador = contador + 1
                    if contador > settings.LIMIT_COMMIT:
                        db.commit()
                        deduplicated_db.commit()
                        contador = 0
                
                # Pegando os campos
                list_unico_entity_fields = get_result_rows(conn=db,sql=f"SELECT DISTINCT type,name,value FROM tb_entity_fields where entity_id in ({list_entidades_com_aspas_in})")
                for item in list_unico_entity_fields:
                    item_type = item[0]
                    item_name = item[1]
                    item_value = item[2]
                    
                    sql_insert = f"INSERT INTO tb_entity_fields_deduplicated (entity_id, type, name, value) VALUES ('{entity_id_master}', '{item_type}', '{item_name}', '{item_value}');"
                    execute_sql(conn=deduplicated_db,sql=sql_insert)
                    contador = contador + 1
                    if contador > settings.LIMIT_COMMIT:
                        db.commit()
                        deduplicated_db.commit()
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
                
                # Pegando as entidade deduplicadas
                de_entity_id_new = get_result_rows(conn=deduplicated_db,sql=f"SELECT DISTINCT entity_id_para FROM tb_de_para where entity_id_de = '{de_entity_id_old}'")[0][0]
                para_entity_id_new = get_result_rows(conn=deduplicated_db,sql=f"SELECT DISTINCT entity_id_para FROM tb_de_para where entity_id_de = '{para_entity_id_old}'")[0][0]
                
                if de_entity_id_new in ja_realizado:
                    if ja_realizado[de_entity_id_new] == para_entity_id_new:
                        continue
                
                ja_realizado[de_entity_id_new] = para_entity_id_new
                
                sql_insert = f"INSERT INTO tb_entity_relations_deduplicated (de_entity_id, para_entity_id, type) VALUES ('{de_entity_id_new}', '{para_entity_id_new}', '{type}');"
                execute_sql(conn=deduplicated_db,sql=sql_insert)
                contador = contador + 1
                if contador > settings.LIMIT_COMMIT:
                    db.commit()
                    deduplicated_db.commit()
                    contador = 0
            except Exception as ex:
                    status_message.error(f"❌ {ex}")  
    
    db.commit()
    deduplicated_db.commit()
    
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
        
    
    depois = get_result_rows(conn=deduplicated_db,sql="""
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
    
    db.close()
    deduplicated_db.close()
    
    # Salva resultados
    result.to_csv('resultado_deduplicado.csv', sep=';', index=False)
        
    return  pd.DataFrame(dicionario)
        
       

    
    