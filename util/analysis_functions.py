from sqlite3 import Connection
import pandas as pd
import streamlit as st
import xml.etree.ElementTree as ET
from dash_util import build_card
from ulti import get_scalar, load_model_relations, load_model_structure

def listing_of_deduplicated_records(entity_type:str, field_name:str, db: Connection,  limit:int):
    st.subheader(f"Listagem das primeiras {limit:,.2f} Entidades (deduplicadas)")
    
    sql = f"""
        SELECT 
            tb_entity_fields_deduplicated.entity_id, 
            MAX(tb_entity_fields_deduplicated.value) AS rotulo,
            min(tb_entity_files.file) as file
        FROM tb_entity_fields_deduplicated
        INNER JOIN tb_entity_files on tb_entity_files.entity_id = tb_entity_fields_deduplicated.entity_id
        WHERE tb_entity_fields_deduplicated.type = '{entity_type}' 
        AND tb_entity_fields_deduplicated.name = '{field_name}'
        GROUP BY tb_entity_fields_deduplicated.entity_id
        ORDER BY rotulo 
        LIMIT {limit};
    """
    df_deduplicated = pd.read_sql_query(sql, db)
    
    if not df_deduplicated.empty:
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")
    else:
        st.warning(f"Nenhum registro foi encontrado")
    
    st.markdown("---")
    
def totalizar_de_entidade(entity_type:str,db: Connection, deduplicated_db: Connection):
    st.subheader(f"Total de registros por tipo de entidade:{entity_type}")
    
    coluna_01, coluna_02 = st.columns([1, 1 ]) 

    
    with coluna_01:
        total = int(get_scalar(conn=db,sql=f"""SELECT 
                                                count(entity_id) as total 
                                            FROM vw_entidades 
                                            where type = '{entity_type}' """))

        
        
        st.markdown(
                build_card(label="Total de Entidades",total=total),
                unsafe_allow_html=True
            )
    
    with coluna_02:
        total = int(get_scalar(conn=deduplicated_db,sql=f"""SELECT count(entity_id) as total 
                                                           FROM vw_entidades_deduplicated 
                                                           where type = '{entity_type}'"""))
        st.markdown(
                build_card(label="Total de Entidades únicas",total=total),
                unsafe_allow_html=True
            )

    st.markdown("---")
      

def brcriss_duplicado(entity_type:str,db: Connection):
    st.subheader(f"Entidades com Identificadores Semanticos duplicados")
    
    df_deduplicated = pd.read_sql_query(f"""
                                        
    SELECT
        vw_semantic_identifier_duplicado_deduplicated.entity_id, 
        vw_semantic_identifier_duplicado_deduplicated.namespace, 
        vw_semantic_identifier_duplicado_deduplicated.total,
        min(tb_entity_files.file) as file
    FROM vw_semantic_identifier_duplicado_deduplicated
    INNER JOIN tb_entity_files on vw_semantic_identifier_duplicado_deduplicated.entity_id = tb_entity_files.entity_id
    WHERE vw_semantic_identifier_duplicado_deduplicated.namespace = 'brcris'
    AND vw_semantic_identifier_duplicado_deduplicated.entity_type = '{entity_type}'
    GROUP BY
        1,2,3                      
                                        """, db)

    if not df_deduplicated.empty:
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")
    else:
        st.warning(f"Nenhum registro foi encontrado")
    
    st.markdown("---")
        

def quantidade_campos_apos_deduplicacao(entity_type:str,db: Connection):
    st.subheader(f"Total de campos por registro após deduplicação")
    
    df_deduplicated = pd.read_sql_query(f"""
                                        
    SELECT 
        tb_entity_fields_deduplicated.entity_id,
        count(tb_entity_fields_deduplicated.id) as total
    FROM tb_entity_fields_deduplicated
    WHERE 
        type = '{entity_type}'
    GROUP BY
        tb_entity_fields_deduplicated.entity_id     
    ORDER BY
        2 desc                 
                                        """, db)

    if not df_deduplicated.empty:
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")
    else:
        st.warning(f"Nenhum registro foi encontrado")
    
    st.markdown("---")
    
    
def total_entidades_este_tipo_entidade_relaciona(entity_type:str,db: Connection):
    st.subheader(f"Total de entidades que este tipo dentidade relaciona")
    
    df_deduplicated = pd.read_sql_query(f"""
                                        
    SELECT 
        vw_entidades_deduplicated.entity_id,
        tb_entity_relations_deduplicated.type,
        count(tb_entity_relations_deduplicated.id) as total
    FROM vw_entidades_deduplicated
    INNER JOIN tb_entity_relations_deduplicated on tb_entity_relations_deduplicated.de_entity_id = vw_entidades_deduplicated.entity_id
    WHERE 
        vw_entidades_deduplicated.type = '{entity_type}'
    GROUP BY
        vw_entidades_deduplicated.entity_id,
        tb_entity_relations_deduplicated.type
    ORDER BY
        3 desc
                                        """, db)

    if not df_deduplicated.empty:
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")
    else:
        st.warning(f"Nenhum registro foi encontrado")
    
    st.markdown("---")
    

def total_entidades_que_relacionam_com_esta_entidade(entity_type:str,db: Connection):
    st.subheader(f"Total de entidades que relacionam com esta entidade")
    
    df_deduplicated = pd.read_sql_query(f"""
                                        
    SELECT 
        vw_entidades_deduplicated.entity_id,
        tb_entity_relations_deduplicated.type,
        count(tb_entity_relations_deduplicated.id) as total
    FROM vw_entidades_deduplicated
    INNER JOIN tb_entity_relations_deduplicated on tb_entity_relations_deduplicated.para_entity_id = vw_entidades_deduplicated.entity_id
    WHERE 
        vw_entidades_deduplicated.type = '{entity_type}'
    GROUP BY
        vw_entidades_deduplicated.entity_id,
        tb_entity_relations_deduplicated.type
    ORDER BY
        3 desc
                                        """, db)

    if not df_deduplicated.empty:
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")
    else:
        st.warning(f"Nenhum registro foi encontrado")
    
    st.markdown("---")
    
    
def brcriss_mal_formatado(entity_type:str, db: Connection,  limit:int):
    st.subheader(f"Entidades com BrCriss Id mal formatados")
    
    sql = f"""                                
    SELECT 
        tb_semantic_identifier_deduplicated.entity_id,
        tb_semantic_identifier_deduplicated.identifier,
        min(tb_entity_files.file) as file
    FROM 
        tb_semantic_identifier_deduplicated
        inner join tb_entity_files on tb_entity_files.entity_id = tb_semantic_identifier_deduplicated.entity_id
    WHERE
        substr(tb_semantic_identifier_deduplicated.identifier, 1, instr(tb_semantic_identifier_deduplicated.identifier, '::') - 1) = 'brcris'
        and length(tb_semantic_identifier_deduplicated.identifier)  > 108
        and tb_semantic_identifier_deduplicated.type = '{entity_type}'  
    GROUP BY 
            1,2  
    LIMIT {limit}     
    """
    
                                    
    df = pd.read_sql_query(sql, db)
    
    if not df.empty:
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
    else:
        st.warning(f"Nenhum registro foi encontrado")
    
    st.markdown("---")
    


def validar_conformidade_com_modelo(entity_type:str, db: Connection, model_path:str,  limit:int):
    st.subheader(f"Validar a conformidade com o modelo")
    
    model_structure = load_model_structure(model_path)
    model_structure_relation = load_model_relations(model_path)
    
    if entity_type not in model_structure:
        st.error(f"Entidade {entity_type} não definida no modelo")
        return
    
    # Percorrer todo o modelo procurando a entidade desejada
    lista_campos_modelo = []
    model_fields = model_structure[entity_type]
    for field in model_fields:
        lista_campos_modelo.append(f"'{field}'")
    
    lista_campos_modelo_in = ','.join(lista_campos_modelo)
    
    lista_relacionamentos_modelo = []
    for relation in model_structure_relation:
        lista_relacionamentos_modelo.append(f"'{relation}'")
    
    lista_relacionamentos_modelo_in = ','.join(lista_relacionamentos_modelo)
                                    
    df_campos = pd.read_sql_query(f"""                                
        SELECT 
            tb_entity_fields_deduplicated.entity_id, 
            tb_entity_fields_deduplicated.name, 
            min(tb_entity_files.file) AS file
        FROM tb_entity_fields_deduplicated
        INNER JOIN tb_entity_files on tb_entity_files.entity_id = tb_entity_fields_deduplicated.entity_id
        WHERE tb_entity_fields_deduplicated.type = '{entity_type}'
        AND tb_entity_fields_deduplicated.name NOT IN ({lista_campos_modelo_in})
        GROUP BY 
            1,2
        LIMIT {limit}
                
        """, db)
    if not df_campos.empty:
        st.subheader(f"Campos inválidos:")
        st.dataframe(df_campos) 
        st.info(f"Total de linhas: {len(df_campos.index):.2f}")
    else:
        st.warning(f"Todos os campos então em conformidade com o modelo")
        
    
    
    df_relacionamentos = pd.read_sql_query(f"""                                
        SELECT 
            tb_entity_relations_deduplicated.de_entity_id, 
            tb_entity_relations_deduplicated.type, 
            min(tb_entity_files.file) AS file
        FROM tb_entity_relations_deduplicated
        INNER JOIN tb_entity_files on tb_entity_files.entity_id = tb_entity_relations_deduplicated.de_entity_id
        WHERE  tb_entity_relations_deduplicated.type NOT IN ({lista_relacionamentos_modelo_in})
        GROUP BY 
            1,2
        LIMIT {limit}
                
        """, db)
    if not df_relacionamentos.empty:
        st.subheader(f"Relacionamentos inválidos:")
        st.dataframe(df_relacionamentos) 
        st.info(f"Total de linhas: {len(df_relacionamentos.index):.2f}")
    else:
        st.write('ta vazio')
        st.warning(f"Todos os relacionamentos então em conformidade com o modelo")
    
   
    st.markdown("---")