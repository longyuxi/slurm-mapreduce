import sys
from pathlib import Path
import argparse
import traceback

from tqdm import tqdm
from trivialdb import DB

def job(key):
    # Given information about the job, do the actual work here
    d = DB.hgetall(key)
    print(d)

    # For example, we can pretend to do some work here
    y = int(d['x']) ** 2


    DB.hset(key, mapping={
        **d,
        'y': y
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
