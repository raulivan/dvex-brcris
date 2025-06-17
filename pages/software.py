import os
import plotly.express as px 
import streamlit as st
import pandas as pd

from dash_util import build_card
from ulti import connect_local_database, get_scalar

st.set_page_config(
    page_title="Software", 
    page_icon="💾", 
    layout="wide"              # (Opcional) Pode ser "centered" (padrão) ou "wide" para ocupar mais largura
)

st.title("Análise do conjunto de dados Software")
st.write("Aqui você poderá ver gráficos e tabelas com os dados extraídos dos seus arquivos XML.")

def __build_char_depositDate(db):
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
        
    df_grafico = pd.read_sql_query(query_grafico, db)
        
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

def __build_totalizadores(db):

    coluna_01, coluna_02 = st.columns([1, 1 ]) # Com 2 colunas iguais

    
    with coluna_01:
        total = int(get_scalar(conn=db,sql="SELECT count(DISTINCT entity_id) as total FROM tb_entity_fields where type = 'Software' "))
        # formatted_total_registros = f"{total:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        # st.metric(label="Total de Entidades", value=formatted_total_registros)
        
        
        st.markdown(
                build_card(label="Total de Entidades",total=total),
                unsafe_allow_html=True
            )
    
    with coluna_02:
        total = int(get_scalar(conn=db,sql="SELECT count(DISTINCT entity_id) as total FROM tb_entity_fields_deduplicated where type = 'Software'"))
        st.markdown(
                build_card(label="Total de Entidades únicas",total=total),
                unsafe_allow_html=True
            )

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

            
# Botão para carregar os dados
if st.button("Atualizar"):
    status_message = st.empty()
    
    with st.spinner(f"Carregando os dados..."):
        try:
            db = connect_local_database()
            
            df = pd.read_sql_query(f"SELECT * FROM tb_entity_fields where type = 'Software' order by entity_id, name limit 500000", db)

            # 4. Exibir no Streamlit
            if not df.empty:
                status_message.success(f"Dados carregados com sucesso! ✅")
                st.subheader(f"Entidade geradas")
                st.dataframe(df) 
                st.info(f"Total de linhas: {len(df.index):.2f}")
            else:
                status_message.warning(f"Não existem dados para serem exibidos. ⚠️")

            st.markdown("---")
            
            # Gerando os totalizadores
            __build_totalizadores(db)
            
            st.markdown("---")
            
            # Gerando gráfico de barras
            __build_char_depositDate(db)  
            
            st.markdown("---") 
            
            # Verificando a quantidade de relacionamentos da entidade
            __build_total_relacionamentos(db)     
            
            st.markdown("---")

            db.close()

        except Exception as e:
            status_message.error(f"Ocorreu um erro inesperado: {e} ❌")