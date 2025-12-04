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

from util.analysis_functions import brcriss_duplicado, brcriss_mal_formatado, listing_of_deduplicated_records, quantidade_campos_apos_deduplicacao, total_entidades_este_tipo_entidade_relaciona, total_entidades_que_relacionam_com_esta_entidade, totalizar_de_entidade, validar_conformidade_com_modelo

ENTITY_TYPE = 'OrgUnit'

st.set_page_config(
    page_title=ENTITY_TYPE, 
    page_icon="💾", 
    layout="wide"              # (Opcional) Pode ser "centered" (padrão) ou "wide" para ocupar mais largura
)

st.title("Análise do conjunto de dados Organizações")

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

brcris_model_path = st.text_input(
        "Caminho do BrCris Model:",
        value=r'C:\worksapce\IBICT\brcris-model\modelo_brcris.xml',
        placeholder="Digite o caminho completo aqui...",
        key="xml_brcrismodel_input"
    )

valor_numerico = st.number_input(
    "Informe o limite de registros a serem exibidos nas listagens (valor numérico entre 0 e 500.000):", # Rótulo para o input
    min_value=0,                                     # Valor mínimo permitido
    max_value=500000,                                # Valor máximo permitido
    value=500000,                                    # Valor inicial padrão
    step=1000                                        # Incremento de 1000
)


# Botão para carregar os dados
if st.button("Atualizar"):
    status_message = st.empty()
    
    with st.spinner(f"Carregando os dados..."):
        try:
            db = connect_local_database()
            deduplicated_db = connect_deduplicated_database()
            
            # Total de Registros da Entidade Gerada
            totalizar_de_entidade(entity_type=ENTITY_TYPE, db=db,deduplicated_db = deduplicated_db)
            
            # Verifica se todo os campos estão dentro do modelo
            validar_conformidade_com_modelo(entity_type=ENTITY_TYPE, db=deduplicated_db,model_path=brcris_model_path, limit=valor_numerico)
            
            # Listagem de entidade deduplicadas
            listing_of_deduplicated_records(entity_type=ENTITY_TYPE,field_name='name', db=deduplicated_db, limit=valor_numerico)
                          
            # BrCriss mal formatado
            brcriss_mal_formatado(entity_type=ENTITY_TYPE, db=deduplicated_db, limit=valor_numerico)

            # BrCriss Id duplicado
            brcriss_duplicado(entity_type=ENTITY_TYPE, db=deduplicated_db)
            
            # Quantidade de campos após a deduplicação
            quantidade_campos_apos_deduplicacao(entity_type=ENTITY_TYPE, db=deduplicated_db)
            
            # Total de registro que este entidade se relaciona
            total_entidades_este_tipo_entidade_relaciona(entity_type=ENTITY_TYPE, db=deduplicated_db)
            
            # Total de entidades que relacionam com esta entidade
            total_entidades_que_relacionam_com_esta_entidade(entity_type=ENTITY_TYPE, db=deduplicated_db)
            
                       
                            
            db.close()
            deduplicated_db.close()

        except Exception as e:
            status_message.error(f"Ocorreu um erro inesperado: {e} ❌")
        
        status_message.success(f"Dados carregados com sucesso! ✅")


    
