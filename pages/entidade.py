import os
import plotly.express as px 
import streamlit as st
import pandas as pd
import settings
from dash_util import build_card
from ulti import connect_deduplicated_database, connect_local_database, get_scalar

st.set_page_config(
    page_title="Entidade", 
    page_icon="📋", 
    layout="wide"            
)

st.title("Entidade")

txt_entity_id = st.text_input(
    "Entity ID:",
    value='',
    placeholder="Digite o Entity Id...",
    key="txt_entity_id"
)


def __build_identificadores_semanticos(db,deduplicated_db, entity_id:str):
    
    df = pd.read_sql_query(f"""
           SELECT identifier 
            FROM 
            tb_semantic_identifier_deduplicated where entity_id =  '{entity_id}'
            order by identifier
                           """, deduplicated_db)
    if not df.empty:
        st.subheader(f"Identificadores semânticos após deduplicação")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
    
    df2 = pd.read_sql_query(f"""
           SELECT DISTINCT identifier 
            FROM 
            tb_semantic_identifier where entity_id =  '{entity_id}'
           order by identifier
                           """, db)
    if not df2.empty:
        st.subheader(f"Identificadores antes da deduplicação")
        st.dataframe(df2) 
        st.info(f"Total de linhas: {len(df2.index):.2f}")

def __build_atributos(db,deduplicated_db, entity_id:str):
    
    df = pd.read_sql_query(f"""
           SELECT DISTINCT name, value 
            FROM 

            tb_entity_fields_deduplicated where entity_id = '{entity_id}'
            
            order by name

                           """, deduplicated_db)
    if not df.empty:
        st.subheader(f"Atributos da entidade após deduplicação")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
    
    df2 = pd.read_sql_query(f"""
           SELECT DISTINCT name, value 
            FROM 

            tb_entity_fields where entity_id = '{entity_id}'
            
            order by name

                           """, db)
    if not df2.empty:
        st.subheader(f"Atributos da entidade antes da deduplicação")
        st.dataframe(df2) 
        st.info(f"Total de linhas: {len(df2.index):.2f}")

def __build_relacionamentos(db,deduplicated_db, entity_id:str):
    
    df = pd.read_sql_query(f"""
           SELECT DISTINCT para_entity_id,type 
            FROM 
            tb_entity_relations_deduplicated where de_entity_id = '{entity_id}' 
           

                           """, deduplicated_db)
    if not df.empty:
        st.subheader(f"Essa entidade relaciona com (após deduplicação):")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
        
    df2 = pd.read_sql_query(f"""
           SELECT DISTINCT para_entity_id,type 
            FROM 
            tb_entity_relations where de_entity_id = '{entity_id}' 
           

                           """, db)
    if not df2.empty:
        st.subheader(f"Essa entidade relaciona com (antes da deduplicação):")
        st.dataframe(df2) 
        st.info(f"Total de linhas: {len(df2.index):.2f}")
        
    
    
    df3 = pd.read_sql_query(f"""
                           
           SELECT de_entity_id,type FROM tb_entity_relations_deduplicated where para_entity_id = '{entity_id}' 

                           """, deduplicated_db)
    if not df3.empty:
        st.subheader(f"Entidades que relacionam esta entidade após deduplicação:")
        st.dataframe(df3) 
        st.info(f"Total de linhas: {len(df3.index):.2f}")
        
    df4 = pd.read_sql_query(f"""
                           
           SELECT de_entity_id,type FROM tb_entity_relations where para_entity_id = '{entity_id}' 

                           """, db)
    if not df4.empty:
        st.subheader(f"Entidades que relacionam esta entidade antes da deduplicação:")
        st.dataframe(df4) 
        st.info(f"Total de linhas: {len(df4.index):.2f}")


def __build_arquivos(db, entity_id:str):
    
    df = pd.read_sql_query(f"""
           
           select DISTINCT  file
           FROM           
           (SELECT de_file as file FROM tb_de_para where entity_id_para =  '{entity_id}' 
           union all 
           SELECT para_file as file FROM tb_de_para where entity_id_para =  '{entity_id}' 
           union all 
           SELECT de_file as file FROM tb_de_para where entity_id_de =  '{entity_id}' 
           union all 
           SELECT para_file as file FROM tb_de_para where entity_id_de =  '{entity_id}') as tabela 


                           """, db)
    if not df.empty:
        st.subheader(f"Arquivos relacionados")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")

def __build_entidades_combinadas(db, entity_id:str):
    
    df = pd.read_sql_query(f"""
                           
            SELECT file FROM tb_entity_files where entity_id= '{entity_id}' 

                           """, db)
    if not df.empty:
        st.subheader(f"Entidades combinadas")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
    else:
        st.warning("Entidade elminada no processo de deduplicação")
                
if st.button("Visualizar"):
    st.subheader(f"Detalhes da entidade: {txt_entity_id}")
    with st.spinner(f"Carregando os dados..."):
        
        db = connect_local_database()
        deduplicated_db = connect_deduplicated_database()
        
        __build_identificadores_semanticos(db=db,deduplicated_db=deduplicated_db, entity_id=txt_entity_id)
        
        st.markdown("---")
        
        __build_atributos(db=db,deduplicated_db=deduplicated_db,entity_id=txt_entity_id)
        
        st.markdown("---")
        
        __build_relacionamentos(db=db,deduplicated_db=deduplicated_db,entity_id=txt_entity_id)
        
        st.markdown("---")

        __build_entidades_combinadas(db=deduplicated_db,entity_id=txt_entity_id)
        
        st.markdown("---")
        
        __build_arquivos(db=depara_db,entity_id=txt_entity_id)
        
        db.close()
        depara_db.close()
        deduplicated_db.close()
    