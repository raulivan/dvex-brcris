import os
import plotly.express as px 
import streamlit as st
import pandas as pd
import settings
from dash_util import build_card
from ulti import connect_deduplicated_database, connect_local_database, get_scalar

from streamlit_modal import Modal
import webbrowser # NOVO: Para abrir links em nova aba
import urllib.parse

from util.analysis_functions import listing_of_deduplicated_records, totalizar_de_entidade

ENTITY_TYPE = 'Software'

st.set_page_config(
    page_title=ENTITY_TYPE, 
    page_icon="💾", 
    layout="wide"              # (Opcional) Pode ser "centered" (padrão) ou "wide" para ocupar mais largura
)

st.title("Análise do conjunto de dados Software")
st.write("Aqui você poderá ver gráficos e tabelas com os dados extraídos dos seus arquivos XML.")

xml_file_path = st.text_input(
        "Caminho do banco de dados local:",
        value=settings.LOCAL_DATABASE_PATH,
        key="xml_directory_input",
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
    value=500000,                                    # Valor inicial padrão
    step=1000                                        # Incremento de 1000
)



        
def __build_table_duplicated_identifiers(db):
    df_deduplicated = pd.read_sql_query(f"""
                                        
                SELECT entity_id, namespace, COUNT(valor) as total
                FROM
                (SELECT 
                    substr(identifier, 1, instr(identifier, '::') - 1) AS namespace,
                    substr(identifier, instr(identifier, '::') + 2) AS valor,
                    entity_id,
                    type
                FROM 
                    tb_semantic_identifier_deduplicated
                WHERE 
                    type = 'Software') as tabela
                group by namespace, entity_id
                having COUNT(valor) > 1
                limit {valor_numerico}                          
                                        """, db)

    if not df_deduplicated.empty:
        st.subheader(f"Entidades com Identificadores Semanticos duplicados")
        st.dataframe(df_deduplicated) 
        st.info(f"Total de linhas: {len(df_deduplicated.index):.2f}")


def __build_total_merge(db):
    
    df = pd.read_sql_query(f"""
                           
            SELECT dp.entity_id_para,e.type, count(dp.entity_id_de) as total 
            FROM tb_de_para dp
            INNER JOIN vw_entidades_deduplicated  e on dp.entity_id_para = e.entity_id
            group by 1,2
            order by 2, 3 desc

                           """, db)
    if not df.empty:
        st.subheader(f"Total de entidade combinadas após deduplicação")
        df_filtered = df[df['type'] == 'Software']
        st.dataframe(df_filtered) 
        st.info(f"Total de linhas: {len(df.index):.2f}")
                                
def __build_char_depositDate(db, deduplicated_db):
    query_grafico = """
    SELECT tab.ano, count(tab.entity_id) as total
    FROM
        (SELECT DISTINCT entity_id, SUBSTR(value, -4) as ano
        FROM tb_entity_fields
        WHERE type = 'Software' AND name = 'depositDate') AS tab
    GROUP BY 1
    ORDER BY 1
    """
        
    df_grafico = pd.read_sql_query(query_grafico, db)
        
    if not df_grafico.empty:
        
        
        fig = px.bar(
            df_grafico,
            x='ano',# Eixo X
            y='total',# Eixo Y
            labels={'ano': 'Ano', 'total': 'Quantidade de Softwares'}, # Rótulos dos eixos
            title='Total de Softwares Depositados por depositDate', # Título do gráfico
            hover_data={'total': True}, # Mostra o valor de 'total' ao passar o mouse
            color_discrete_sequence=px.colors.qualitative.Pastel ,# Uma paleta de cores suave
            text_auto=True
        )
        
        # Ajusta o layout para melhor visualização 
        fig.update_layout(xaxis_title="Ano do Depósito", yaxis_title="Número de Softwares")
        fig.update_xaxes(type='category') # Garante que os anos sejam tratados como categorias, não números contínuos
        
        # Exibe o gráfico no Streamlit
        st.plotly_chart(fig, use_container_width=True) # use_container_width ajusta à largura da coluna
    
    query_grafico = """
    SELECT tab.ano, count(tab.entity_id) as total
    FROM
        (SELECT DISTINCT entity_id, SUBSTR(value, -4) as ano
        FROM tb_entity_fields_deduplicated
        WHERE type = 'Software' AND name = 'depositDate') AS tab
    GROUP BY 1
    ORDER BY 1
    """
        
    df_grafico = pd.read_sql_query(query_grafico, deduplicated_db)
        
    if not df_grafico.empty:        
        fig = px.bar(
            df_grafico,
            x='ano',# Eixo X
            y='total',# Eixo Y
            labels={'ano': 'Ano', 'total': 'Quantidade de Softwares'},
            title='Total de Softwares Depositados por depositDate (Únicos)', 
            hover_data={'total': True}, 
            color_discrete_sequence=px.colors.qualitative.Pastel ,
            text_auto=True
        )
        
        fig.update_layout(xaxis_title="Ano do Depósito", yaxis_title="Número de Softwares")
        fig.update_xaxes(type='category') 
        

        st.plotly_chart(fig, use_container_width=True)



def __build_total_relacionamentos(db):
    
    df = pd.read_sql_query(f"""
        SELECT e.entity_id, r.type,count(r.id) as total
        FROM vw_entidades_deduplicated e
        INNER JOIN tb_entity_relations_deduplicated r on e.entity_id = r.de_entity_id
        WHERE e.type = 'Software' 
        GROUP BY 1,2 
        order by 3 desc, 1,2
        """, db)
    
    if not df.empty:
        st.subheader(f"Total de relacionamentos após deduplicação")
        st.dataframe(df) 
        st.info(f"Total de linhas: {len(df.index):.2f}")



def __build_total_atributos(db):
    
    df = pd.read_sql_query(f"""
                           
            SELECT f.entity_id,count(f.id) as total
            FROM tb_entity_fields_deduplicated f
            WHERE f.type = 'Software' 
            GROUP BY 1
            order by 2 desc, 1

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
            deduplicated_db = connect_deduplicated_database()
            
            totalizar_de_entidade(entity_type=ENTITY_TYPE, db=db,deduplicated_db = deduplicated_db)
            
            st.markdown("---")
            
            # Listagem de entidade deduplicadas
            listing_of_deduplicated_records(entity_type=ENTITY_TYPE,field_name='title.lattes', db=deduplicated_db, limit=valor_numerico)              
            
            st.markdown("---")
            
            
    
            
            
            st.markdown("---")
            # Identificadores semanticos duiplicados
            st.info("Indicador #3")
            __build_table_duplicated_identifiers(deduplicated_db)   

            st.markdown("---")
            
            # Gerando os totalizadores
            st.info("Indicador #4")
            __build_totalizadores(db,deduplicated_db)
            
            st.markdown("---")
            
            # Gerando gráfico de barras
            st.info("Indicador #5")
            __build_char_depositDate(db, deduplicated_db)  
            
            st.markdown("---") 
            
            # Exibe o total de rotinas combinadas
            st.info("Indicador #6")
            __build_total_merge(deduplicated_db)
            
            st.markdown("---")
            
            # Verificando a quantidade de relacionamentos da entidade
            st.info("Indicador #7")
            __build_total_relacionamentos(deduplicated_db)     
            
            st.markdown("---")
            
            # Verifica a quantidade de atributos
            st.info("Indicador #8")
            __build_total_atributos(deduplicated_db)
            
            st.markdown("---")
            
            
            
                            
            db.close()
            deduplicated_db.close()

        except Exception as e:
            status_message.error(f"Ocorreu um erro inesperado: {e} ❌")
        
        status_message.success(f"Dados carregados com sucesso! ✅")


    
