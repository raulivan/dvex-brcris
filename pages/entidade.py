import os
import plotly.express as px 
import streamlit as st
import pandas as pd
import settings
from dash_util import build_card
from ulti import connect_local_database, get_scalar

st.set_page_config(
    page_title="Entidade", 
    page_icon="📋", 
    layout="wide"            
)

st.title("Entidade")

txt_entity_id = st.text_input(
    "Entity ID:",
    value='53c495f3b45d4d09ac21f92b521b5877',
    placeholder="Digite o Entity Id...",
    key="txt_entity_id"
)


def __build_identificadores_semanticos(db, entity_id:str):
    
    df = pd.read_sql_query(f"""
                           
           SELECT identifier FROM tb_semantic_identifier_deduplicated where entity_id = '{entity_id}'

                           """, db)
    if not df.empty:
        st.subheader(f"Identificadores semânticos")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")

def __build_atributos(db, entity_id:str):
    
    df = pd.read_sql_query(f"""
                           
           SELECT name, value FROM tb_entity_fields_deduplicated where entity_id = '{entity_id}' order by name

                           """, db)
    if not df.empty:
        st.subheader(f"Atributos da entidade")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")

def __build_relacionamentos(db, entity_id:str):
    
    df = pd.read_sql_query(f"""
                           
           SELECT para_entity_id,type FROM tb_entity_relations_deduplicated where de_entity_id = '{entity_id}' 

                           """, db)
    if not df.empty:
        st.subheader(f"Essa entidade relaciona com:")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
        
    
    
    df2 = pd.read_sql_query(f"""
                           
           SELECT de_entity_id,type FROM tb_entity_relations_deduplicated where para_entity_id = '{entity_id}' 

                           """, db)
    if not df2.empty:
        st.subheader(f"Entidades que relacionam esta entidade:")
        st.dataframe(df2) 
        st.info(f"Total de linhas: {len(df2.index):.2f}")


def __build_arquivos(db, entity_id:str):
    
    df = pd.read_sql_query(f"""
                           
           SELECT file FROM tb_de_para where entity_id_para =  '{entity_id}' 

                           """, db)
    if not df.empty:
        st.subheader(f"Arquivos relacionados")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
                
if st.button("Visualizar"):
    with st.spinner(f"Carregando os dados..."):
        db = connect_local_database()
        
        __build_identificadores_semanticos(db=db,entity_id=txt_entity_id)
        
        st.markdown("---")
        
        __build_atributos(db=db,entity_id=txt_entity_id)
        
        st.markdown("---")
        
        __build_relacionamentos(db=db,entity_id=txt_entity_id)
        
        st.markdown("---")
    
        __build_arquivos(db=db,entity_id=txt_entity_id)
    