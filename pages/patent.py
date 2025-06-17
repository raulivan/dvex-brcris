import streamlit as st
import pandas as pd

from ulti import connect_local_database

st.set_page_config(
    page_title="Software", 
    page_icon="💾", 
    layout="wide"              # (Opcional) Pode ser "centered" (padrão) ou "wide" para ocupar mais largura
)

st.title("Análise do conjunto de dados Patent")
st.write("Aqui você poderá ver gráficos e tabelas com os dados extraídos dos seus arquivos XML.")


# Botão para carregar os dados
if st.button("Atualizar"):
    
    # Placeholder para mensagens de status
    status_message = st.empty()

    # Usando st.spinner para indicar processamento
    with st.spinner(f"Carregando os dados da tabela..."):
        try:
           
            conn = connect_local_database()
            
            df = pd.read_sql_query(f"SELECT * FROM tb_entity_fields where type = 'Patent' order by entity_id, name", conn)

            # 4. Exibir no Streamlit
            if not df.empty:
                status_message.success(f"Dados carregados com sucesso! ✅")
                st.subheader(f"Entidade Patent`")
                st.dataframe(df) # st.dataframe é mais interativo que st.table
                st.info(f"Total de linhas: {len(df.index)}")
            else:
                status_message.warning(f"Não existem dados para serem exibidos. ⚠️")

            conn.close() # Sempre fechar a conexão!

        except Exception as e:
            status_message.error(f"Ocorreu um erro inesperado: {e} ❌")