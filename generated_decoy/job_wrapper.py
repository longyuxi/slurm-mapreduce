import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / '..'))
from db import DB
from preprocessing import get_mol2_coordinates

import argparse
import traceback
from tqdm import tqdm


def job(key):
    # The part where the job actually runs, given df and idx as input

    # Each entry in the redis database should be a dictionary in the following form

    d = DB.hgetall(key)
    print(d)

    protein_file = d['protein_file']
    ligand_file = d['ligand_file']

    num_protein_atoms = len(get_mol2_coordinates(d['protein_file']))
    num_ligand_atoms = len(get_mol2_coordinates(d['ligand_file']))
    total_atoms = num_protein_atoms + num_ligand_atoms

    # Implement the actual job here
    raise NotImplementedError

    DB.hset(key, mapping={
        **d,
        'num_protein_atoms': str(num_protein_atoms),
        'num_ligand_atoms': str(num_ligand_atoms),
        'total_atoms': str(total_atoms),
    })


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--key')

    args = parser.parse_args()
    key = args.key

    print('key', key)
    print('job started')
    d = DB.hgetall(key)
    d['attempted'] = 'True'
    DB.hset(key, mapping=d)

    try:
        job(key)
        print('job finished')
        d = DB.hgetall(key)
        d['finished'] = 'True'
        d['error'] = 'False'
        DB.hset(key, mapping=d)
        print('job success')

    except Exception as err:
        print(Exception, err)
        print(traceback.format_exc())
        print('job error')

        d = DB.hgetall(key)
        d['finished'] = 'True'
        d['error'] = 'True'
        DB.hset(key, mapping=d)
