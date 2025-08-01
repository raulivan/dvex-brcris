import os
from datetime import timedelta
from pathlib import Path

run_rath = Path(__file__).resolve().parent

# Variáveis de Ambiente
XML_BRCRIS_MODEL_PATH = r'C:\worksapce\IBICT\brcris-model\modelo_brcris.xml'
DATASET_PATH =  r'C:\DATABASE-IBICT\Finalizados\2025\SoftwareLattes' #r'C:\DATABASE-IBICT\Finalizados\2025\demo'
ALLOWED_ORGUNITS_PATH = r'C:\worksapce\IBICT\dvex-brcris\allowed_orgunits.csv'
LOCAL_DATABASE_PATH = f'{run_rath}/db_local.db'
LOCAL_DATABASE_DEPARA_PATH = f'{run_rath}/db_depara.db'
LOCAL_DATABASE_DEDUPLICATED_PATH = f'{run_rath}/db_deduplicated.db'
LIMIT_COMMIT = 100000
#r'C:\DATABASE-IBICT\Finalizados\2025\PatentsLattes'
#r'C:\DATABASE-IBICT\Finalizados\2025\SoftwareLattes'