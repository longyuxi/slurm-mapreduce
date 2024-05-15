## Constants for database and index location

import redis
from pathlib import Path
import sys
sys.path.append('/usr/project/dlab/Users/jaden/PATH')

from preprocessing import load_pdbbind_data_index

CLUSTER = 'CS' # or 'DCC'


if CLUSTER == 'CS':
    DB = redis.Redis(host='fill-in-here', port=6379, decode_responses=True, password='fill-in-here-too')
else:
    raise Exception     # Incorrect specification of cluster variable


if CLUSTER == 'CS':
    INDEX_LOCATION = Path('/usr/project/dlab/Users/jaden/pdbbind/index/INDEX_refined_data.2020')
    PDBBIND_BASE_FOLDER = '/usr/project/dlab/Users/jaden/pdbbind/refined-set'
    GENERATED_DECOYS_BASE_FOLDER = '/usr/project/dlab/Users/jaden/pdbscreen/generated_decoy_pdbs'
    CROSS_DECOYS_BASE_FOLDER = '/usr/project/dlab/Users/jaden/pdbscreen/cross_decoy_pdbs'
else:
    raise NotImplementedError


INDEX = load_pdbbind_data_index(INDEX_LOCATION)

