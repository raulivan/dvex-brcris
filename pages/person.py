import os
import plotly.express as px 
import streamlit as st
import pandas as pd
import settings
from dash_util import build_card
from ulti import connect_deduplicated_database, connect_depara_database, connect_local_database, get_scalar

from streamlit_modal import Modal
import webbrowser # NOVO: Para abrir links em nova aba
import urllib.parse # NOVO: Para codificar URLs

st.set_page_config(
    page_title="Person", 
    page_icon="💾", 
    layout="wide"              # (Opcional) Pode ser "centered" (padrão) ou "wide" para ocupar mais largura
)

st.title("Análise do conjunto de dados Person")
st.write("Aqui você poderá ver gráficos e tabelas com os dados extraídos dos seus arquivos XML.")

xml_file_path = st.text_input(
        "Caminho do banco de dados local:",
        value=settings.LOCAL_DATABASE_PATH,
        key="xml_directory_input",
        disabled=True
    )

xml_file_path2 = st.text_input(
        "Caminho do banco de dados de De/Para:",
        value=settings.LOCAL_DATABASE_DEPARA_PATH,
        key="xml_directory_input2",
        disabled=True
    )

xml_file_path3 = st.text_input(
        "Caminho do banco de dados deduplicado:",
        value=settings.LOCAL_DATABASE_DEDUPLICATED_PATH,
        key="xml_directory_input3",
        disabled=True
    )

valor_numerico = st.number_input(
    "Informe o limite de registros a serem exibidos nas listagens (valor numérico entre 0 e 500.000):", # Rótulo para o input
    min_value=0,                                     # Valor mínimo permitido
    max_value=500000,                                # Valor máximo permitido
    value=1000,                                    # Valor inicial padrão
    step=1000                                        # Incremento de 1000
)


def __build_table_deduplicated(db):
    # pass
    df_deduplicated = pd.read_sql_query(f"SELECT distinct entity_id, value as name FROM tb_entity_fields_deduplicated where type = 'Person' and name = 'name' order by  value limit {valor_numerico}", db)

    if not df_deduplicated.empty:
        st.subheader(f"Primeiras {valor_numerico} Entidades deduplicadas")
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")

def __build_table_duplicated_identifiers(db):
    df_deduplicated = pd.read_sql_query(f"""
                                        
                SELECT entity_id, namespace, COUNT(valor) as total
                FROM
                (SELECT distinct
                    substr(identifier, 1, instr(identifier, '::') - 1) AS namespace,
                    substr(identifier, instr(identifier, '::') + 2) AS valor,
                    entity_id,
                    type
                FROM 
                    tb_semantic_identifier_deduplicated
                WHERE 
                    type = 'Person') as tabela
                group by namespace, entity_id
                having COUNT(valor) > 1
                limit {valor_numerico}                        
                                        """, db)

    if not df_deduplicated.empty:
        st.subheader(f"Entidades com Identificadores Semanticos duplicados")
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")

def __build_totalizadores(db, deduplicated_db):

    coluna_01, coluna_02 = st.columns([1, 1 ]) # Com 2 colunas iguais

    
    with coluna_01:
        total = int(get_scalar(conn=db,sql="SELECT count(entity_id) as total FROM vw_entidades where type = 'Person' "))

        
        
        st.markdown(
                build_card(label="Total de Entidades",total=total),
                unsafe_allow_html=True
            )
    
    with coluna_02:
        total = int(get_scalar(conn=deduplicated_db,sql="SELECT count(entity_id) as total FROM vw_entidades_deduplicated where type = 'Person'"))
        st.markdown(
                build_card(label="Total de Entidades únicas",total=total),
                unsafe_allow_html=True)
                 

def __build_total_merge(db):
    
    df = pd.read_sql_query(f"""
                           
            SELECT dp.entity_id_para,e.type, count(dp.entity_id_de) as total 
            FROM tb_de_para dp
            INNER JOIN vw_entidades_deduplicated  e on dp.entity_id_para = e.entity_id
            group by 1,2
            order by 2, 3 desc
            limit {valor_numerico}     
                           """, db)
    if not df.empty:
        st.subheader(f"Total de entidade combinadas após deduplicação")
        df_filtered = df[df['type'] == 'Person']
        st.dataframe(df_filtered) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
                        

def __build_total_relacionamentos(db):
    
    df = pd.read_sql_query(f"""
        SELECT e.entity_id, r.type,count(r.id) as total
        FROM vw_entidades_deduplicated e
        INNER JOIN tb_entity_relations_deduplicated r on e.entity_id = r.de_entity_id
        WHERE e.type = 'Person' 
        GROUP BY 1,2 
        order by 3 desc, 1,2
        limit {valor_numerico}     
        """, db)
    
    if not df.empty:
        st.subheader(f"Total de relacionamentos após deduplicação")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")

def __build_total_atributos(db):
    
    df = pd.read_sql_query(f"""
                           
            SELECT f.entity_id,count(f.id) as total
            FROM tb_entity_fields_deduplicated f
            WHERE f.type = 'Person' 
            GROUP BY 1
            order by 2 desc, 1
            limit {valor_numerico}     

                           """, db)
    if not df.empty:
        st.subheader(f"Total de atributos por entidade")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
                                 
# Botão para carregar os dados
if st.button("Atualizar"):
    status_message = st.empty()
    
    
    with st.spinner(f"Carregando os dados..."):
        try:
            db = connect_local_database()
            # depara_db = connect_depara_database()
            deduplicated_db = connect_deduplicated_database()
            
            df = pd.read_sql_query(f"SELECT entity_id,value as name, file  FROM tb_entity_fields where type = 'Person' and name = 'name' order by  value limit {valor_numerico}     ", db)

            # Exibir no Streamlit
            if not df.empty:
                st.subheader(f"Primeiras {valor_numerico} Entidades geradas")
                st.dataframe(df) 
                st.info(f"Total de linhas: {len(df.index):.2f}")
                
            else:
                status_message.warning(f"Não existem dados para serem exibidos. ⚠️")
                st.stop() 
                
            
            st.markdown("---")
            
            __build_table_deduplicated(deduplicated_db)
            
            
            # Identificadores semanticos duiplicados
            __build_table_duplicated_identifiers(deduplicated_db)   

            st.markdown("---")
            
              # Gerando os totalizadores
            __build_totalizadores(db,deduplicated_db)
            
            st.markdown("---")
            
            
            # Exibe o total de rotinas combinadas
            # __build_total_merge(deduplicated_db)
            
            st.markdown("---")
            
            # Verificando a quantidade de relacionamentos da entidade
            # __build_total_relacionamentos(deduplicated_db)     
            
            st.markdown("---")
            
            # Verifica a quantidade de atributos
            # __build_total_atributos(deduplicated_db)
            
            st.markdown("---")
            
            
            
            
                            
            db.close()
            deduplicated_db.close()

        except Exception as e:
            status_message.error(f"Ocorreu um erro inesperado: {e} ❌")
        
        status_message.success(f"Dados carregados com sucesso! ✅")