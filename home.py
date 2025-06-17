import streamlit as st
import os
from pathlib import Path
from rule03 import rule_03
from rule04 import rule_04
from rule05 import rule_05
from rule06 import rule_06
from ulti import load_model_structure, load_model_relations
from rule01 import rule_01
from rule02 import rule_02
# import pandas as pd
# 
# 
# from rule02 import rule_02
# from rule03 import rule_03
# from rule04 import rule_04
# import settings


def main():
    
    st.set_page_config(
        page_title="DVEX - BrCris",
        page_icon="📊",
        layout="wide" # Opcional: define o layout da página como wide (mais espaço)
    )
    
    st.title('DVEX - BrCris')
    st.write('<h4>Data Validator and Explorer in the BrCris Model</h4>',unsafe_allow_html=True)
    
    
    
    
    st.write("Por favor, digite o caminho completo do arquivo do BrCrisModel.")
    st.info("Exemplos de caminho: \n- No Windows: `C:\\Users\\SeuUsuario\\Documentos\\modelo_brcris.xml` \n- No Linux/macOS: `/home/seuusuario/documentos/modelo_brcris.xml`")

    brcris_model_path = st.text_input(
        "Caminho do BrCris Model:",
        value=r'C:\worksapce\IBICT\brcris-model\modelo_brcris.xml',
        placeholder="Digite o caminho completo aqui...",
        key="xml_brcrismodel_input"
    )
        
    st.write("Por favor, digite o caminho completo do diretório onde seus arquivos XML estão armazenados.")
    st.info("Exemplos de caminho: \n- No Windows: `C:\\Users\\SeuUsuario\\Documentos\\MeusXMLs` \n- No Linux/macOS: `/home/seuusuario/documentos/meus_xmls`")

    xml_file_path = st.text_input(
        "Caminho do Diretório:",
        value=r'C:\DATABASE-IBICT\Finalizados\2025\SoftwareLattes',
        placeholder="Digite o caminho completo aqui...",
        key="xml_directory_input"
    )
    
    if st.button("Iniciar processo de validação"):
        # Quando clicar no botão
        try:
            if len(brcris_model_path.strip()) != 0 and len(xml_file_path.strip()) != 0: 
                if os.path.isfile(brcris_model_path): 
                    model_structure = load_model_structure(brcris_model_path)
                    model_structure_relation = load_model_relations(brcris_model_path)
                    st.success("Modelo carregado com sucesso!")
                else:
                    st.error(f"O caminho informado não é um arquivo válido ou não existe: `{brcris_model_path}`. Por favor, verifique o caminho e tente novamente.")
                    st.stop()
                
                if os.path.isdir(xml_file_path): 
                    path_base = Path(xml_file_path)
                    files_path = list(path_base.rglob('*.xml'))
                    st.success(f"{len(files_path)} arquivos carregados")
                else:
                    st.error(f"O caminho informado não é um diretório válido ou não existe: `{xml_file_path}`. Por favor, verifique o caminho e tente novamente.")
                    st.stop() 
                
                # df_rule_01 = rule_01(files_path=files_path)
                # if df_rule_01.empty == False:
                #     st.write("Inconformidades identificadas:")
                #     st.write(df_rule_01)
                
                # df_rule_02 = rule_02(files_path=files_path,model_structure=model_structure)
                # if df_rule_02.empty == False:
                #     st.write("Inconformidades identificadas:")
                #     st.write(df_rule_02)
                
                
                # df_rule_03_parte_01, df_rule_03_parte_02 = rule_03(files_path=files_path,model_structure=model_structure,model_relation=model_structure_relation)
                # df_rule_03_parte_01['total'] = df_rule_03_parte_01['total'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
                # st.write('Quantidade de entidades')
                # st.write(df_rule_03_parte_01)
                
                # df_rule_03_parte_02['total'] = df_rule_03_parte_02['total'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
                # st.write('Quantidade de relacionamentos')
                # st.write(df_rule_03_parte_02)
                
                # df_rule_04_parte_01, df_rule_04_parte_02 = rule_04(files_path=files_path)
                # df_rule_04_parte_01['total'] = df_rule_04_parte_01['total'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
                # st.write('OrgUnits válidas')
                # st.write(df_rule_04_parte_01)
                
                # st.write('OrgUnits inválidas')
                # st.write(df_rule_04_parte_02)
                
                
                # rule_05(files_path=files_path)
                
                # df_rule_06 = rule_06(files_path=files_path)
                # df_rule_06['antes'] = df_rule_06['antes'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
                # df_rule_06['depois'] = df_rule_06['depois'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
                
                # st.write('Resultado do processo de deduplicação')
                # st.write(df_rule_06)
                
    
  
                
            else:
                st.warning("Por favor, informe os valores solicitados.")
        except Exception as ex:
            st.error(f"Erro: {ex}")

    
    
    
    # print('Iniciando o processo de validação')
    
    # print('Recuperando o caminho de todos os arquivos XML...')
    # path_base = Path(settings.DATASET_PATH)
    # xml_file_path = list(path_base.rglob('*.xml'))

    # st.title('Validador de conjunto de dados')
    
    # st.title('')
    
    # # REGRA 01
    # df_validade_xml_strcture = rule_01(files_path=xml_file_path)
    # st.write('<h4>1 - Verificando se o conjunto de dados está em conformidade com o Modelo BrCris</h4>',unsafe_allow_html=True)
    # st.write('Inconformidades identificadas')
    # st.write(df_validade_xml_strcture)
    
    # # REGRA 02
    # df_quantitativos, df_quantitativos_relations = rule_02(files_path=xml_file_path)
    # df_quantitativos['total'] = df_quantitativos['total'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
    # st.write('<h4>2 - Quantidade de entidades</h4>',unsafe_allow_html=True)
    # st.write(df_quantitativos)
    
    # df_quantitativos_relations['total'] = df_quantitativos_relations['total'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
    # st.write('<h4>3 - Quantidade de relacionamentos</h4>',unsafe_allow_html=True)
    # st.write(df_quantitativos_relations)
    
    # # REGRA 04
    # df_quantitativos_orgunit, df_invalid_orunits = rule_04(files_path=xml_file_path)
    # df_quantitativos_orgunit['total'] = df_quantitativos_orgunit['total'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
    # st.write('<h4>4 - OrgUnits validadas</h4>',unsafe_allow_html=True)
    # st.write(df_quantitativos_orgunit)
    
    # st.write('<h5>Arquivos com OrgUnits inválidas</h5>',unsafe_allow_html=True)
    # st.write(df_invalid_orunits)
    
    # # REGRA 03
    # df_quantitativos_deduplicate = rule_03(files_path=xml_file_path)
    # df_quantitativos_deduplicate['total'] = df_quantitativos_deduplicate['total'].apply(lambda x: f'{x:,.0f}'.replace(',', '.'))
    # st.write('<h4>5 - Quantidade esperada após deduplicação</h4>',unsafe_allow_html=True)
    # st.write(df_quantitativos_deduplicate)
if __name__ == "__main__":
    main()
