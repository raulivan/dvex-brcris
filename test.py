import os
from rule02 import rule_02
from rule05 import rule_05
import settings
from pathlib import Path
from ulti import load_model_structure, load_model_relations
from rule01 import rule_01

if __name__ == "__main__":
    model_structure = load_model_structure(settings.XML_BRCRIS_MODEL_PATH)
    model_structure_relation = load_model_relations(settings.XML_BRCRIS_MODEL_PATH)
    
    path_base = Path(settings.DATASET_PATH)
    files_path = list(path_base.rglob('*.xml'))

    # df_rule_01 = rule_01(files_path=files_path)
    # print(df_rule_01.head())
    
    # df_rule_02 = rule_02(files_path=files_path,model_structure=model_structure)
    # print(df_rule_02.head())
    
    rule_05(files_path=files_path)
                    
                    
                    