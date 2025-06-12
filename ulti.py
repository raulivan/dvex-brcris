import xml.etree.ElementTree as ET
import csv
import sqlite3
import settings

def load_model_structure(model_file:str):
    """Carrega o arquivo de definição de modelo

    Args:
        model_file (String): Caminho do arquivo de modelo; Ex.: C:\worksapce\IBICT\brcris-model\modelo_brcris.xml

    Returns:
        Dict: Dicionário onde as chaves são as entidade e seu conteudo as definições do campos
    """
    tree = ET.parse(model_file)
    root = tree.getroot()
    model_structure = {}
    
    for entity in root.find('entities'):
        entity_name = entity.get('name')
        fields = {field.get('name'): field for field in entity}
        model_structure[entity_name] = fields
    
    return model_structure

def load_model_relations(model_file:str):
    """Carrega os relacionamentos do arquivo de definição de modelo

    Args:
        model_file (String): Caminho do arquivo de modelo; Ex.: C:\worksapce\IBICT\brcris-model\modelo_brcris.xml

    Returns:
        Dict: Dicionário com os relacionamentos
    """
    tree = ET.parse(model_file)
    root = tree.getroot()
    model_structure = {}
    
    for relation in root.find('relations'):
        relation_name = relation.get('name')
        
        model_structure[relation_name] = {
            'description': relation.get('description'),
            'fromEntity': relation.get('fromEntity'),
            'toEntity': relation.get('toEntity')
        }
    
    return model_structure

def read_csv(path, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL):
    content = []
    with open(path, 'r', newline='',encoding = "utf-8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar) # quoting=quoting
        # next(csv_reader, None)
        for row in csv_reader:
            content.append(row)
    return content 

def connect_local_database():
    return sqlite3.connect(settings.LOCAL_DATABASE_PATH)

def execute_sql(conn: sqlite3.Connection, sql:str):
    cr = conn.cursor()
    try:
        cr.execute(sql)
        cr.close()
        return True
    except Exception as error:
        cr.close()
        raise Exception(f"Erro on execute_sql  Error: {error}") 