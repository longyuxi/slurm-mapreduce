import glob
import os
import pathlib
import logging
import redis
import numpy as np
from tqdm import tqdm
import time
from pathlib import Path
import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument( '-log',
                     '--loglevel',
                     default='warning',
                     help='Provide logging level. Example --loglevel debug, default=warning' )

args = parser.parse_args()

logging.basicConfig( level=args.loglevel.upper() )
logging.info( 'Logging now setup.' )

import sys
from trivialdb import DB


###############################
# Platform specific variables #
#                             #
# Change to fit to job        #
###############################


KEY_PREFIX = 'trivial_' # Prefix of every job, as appears in the Redis database
CLUSTER = 'CS' # or 'DCC'


if CLUSTER == 'CS':
    cwd = pathlib.Path(__file__).parent.resolve()
    NUM_JOBS_TO_SUBMIT = 10
    PYTHON_EXECUTABLE = '/usr/project/dlab/Users/jaden/mambaforge/envs/aa-score/bin/python'
    ROOT_DIR = str(Path(__file__).parent.resolve())
    os.system(f'mkdir -p {ROOT_DIR}/slurm-outs')
    SBATCH_TEMPLATE = f"""#!/bin/zsh
#SBATCH --requeue
#SBATCH --chdir={ROOT_DIR}
#SBATCH --output={ROOT_DIR}/slurm-outs/%x-%j-slurm.out
#SBATCH --mem=8000M
#SBATCH --cpus-per-task=1
#SBATCH --partition=grisman
#SBATCH --exclude=jerry[1-3]
#SBATCH --time=2:00:00

source ~/.zshrc
date
hostname
conda activate aa-score
cd {ROOT_DIR}


    """


    ADDITIONAL_SAVE_FOLDER = ROOT_DIR + '/additional_results'
    os.system(f'mkdir -p {ADDITIONAL_SAVE_FOLDER}')

else:
    raise Exception     # Incorrect specification of cluster variable



#############################
# Pre-execution Tests       #
#############################

# Database connection
DB.set('connection-test', '123')
if DB.get('connection-test') == '123':
    DB.delete('abc')
    logging.info('Database connection successful')
else:
    raise Exception     # Database connection failed


#############################
# Actual logic              #
#############################


def main(dry_run=False, rebuild_all_keys=False):
    # Initialize database on first run
    if dry_run:
        populate_db(rebuild_all_keys=rebuild_all_keys)

    # Then submit jobs until either running out of entries or running out of number of jobs to submit
    i = 0

    database_keys = DB.keys(KEY_PREFIX + '*')
    for key in database_keys:
        if i == NUM_JOBS_TO_SUBMIT:
            break
        info = DB.hgetall(key)

        # Skip if job is already finished
        if info['finished'] == 'True' and info['error'] == 'False':
        # if info['attempted'] == 'True':
            continue
        else:
            i += 1
            # submit job for it
            if not dry_run:
                info['attempted'] = 'True'
                DB.hset(key, mapping=info)

                # sbatch run job wrapper
                sbatch_cmd = SBATCH_TEMPLATE + f'\n{PYTHON_EXECUTABLE} {str(pathlib.Path(__file__).parent) + "/job_wrapper.py"} --key {key}'

                # print(sbatch_cmd)
                with open('run.sh', 'w') as f:
                    f.write(sbatch_cmd)

                os.system(f'sbatch --job-name={key} run.sh')

    if dry_run:
        print(f'Number of jobs that would be submitted: {i}')
        time.sleep(5)
    else:
        print(f'Number of jobs submitted: {i}')


def populate_db(rebuild_all_keys=False):
    # Fetch information. See the scripts for e.g. PDBBind for nontrivial example
    # Here we just generate a list of random numbers and pretend they are the data
    random_numbers = np.random.randint(0, 100, NUM_JOBS_TO_SUBMIT)

    # Populate database
    logging.info('Populating database')
    n_jobs = NUM_JOBS_TO_SUBMIT
    keys = [KEY_PREFIX + str(i) for i in range(n_jobs)]

    database_keys = DB.keys()

    for k in tqdm(keys):
        if not rebuild_all_keys and k in database_keys:
            logging.debug(f"Key {k} already exists in database")
            continue

        if rebuild_all_keys:
            DB.delete(k)


        # Add the entry to the database
        DB.hset(k, mapping={
            # Variables for keeping track of the job
            'attempted': 'False',
            'error': 'False',
            'finished': 'False',

            # Information for the job
            'x': str(random_numbers[int(k[len(KEY_PREFIX):])]),
        })

    print(f'Example key: {keys[0]}')
    print(f'Example entry: {DB.hgetall(keys[0])}')


if __name__ == '__main__':
    main(dry_run=True, rebuild_all_keys=True)
    main(dry_run=False)
