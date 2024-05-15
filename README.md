# MapReduce-style system for SLURM

*I execute my computational tasks (jobs) in a compute cluster managed by SLURM. To keep track of the statuses of jobs and their results, I use a Redis database and a custom MapReduce-style system.*

I first explain my custom MapReduce-style system. This system consists of

- Two scripts:
    - `job_wrapper.py`
    - `dispatch_jobs.py`
- A SLURM scheduler
- A Redis database.

<!-- If you are running these scripts in a SLURM cluster, you will need to modify the headers of the temporary shell scripts (see below) to fit the configuration of your cluster. If you are executing these scripts on a compute cluster with a different job scheduler, more changes will need to be made according how compute jobs are submitted on your cluster. -->

1. Each task is associated with a set of sequentially numbered key starting from a prefix, which is reflected in the `KEY_PREFIX` variable in `dispatch_jobs.py`.
2. `dispatch_jobs.py` will create an entry in the database for each key containing information about the job and the fields {`started`, `finished`, `error`} all set to `False`. It then submits by creating temporary shell scripts that execute `python job_wrapper.py --key {k}` and submit these shell scripts to the SLURM scheduler.
3. `job_wrapper.py` contains the instructions for execution when the work is allocated to a scheduler.

# Setup

As mentioned, a Redis database is used for managing jobs submitted to the SLURM batch cluster. To set up this database,

1. Build and install Redis via [https://redis.io/docs/getting-started/installation/install-redis-from-source/].
2. Optionally, add the `src` folder of Redis to path.
3. Create a `redis.conf` file somewhere and set a default password by putting e.g. `requirepass your-password` in that file.
4. Start the redis server on a host with your `redis.conf` and adjust the `DB` constant in `dispatch_jobs.py` accordingly.

# Getting started

- The `trivial` directory contains skeleton code for the logic of this system.
- The `biolip`, `pdbbind`, `cross_decoy`, `generated_decoy` folders contain accessor scripts for the eponymous datasets. They are largely directly pulled from my binding benchmark scripts for the ADFR tool (`/usr/project/dlab/Users/jaden/binding-benchmarks/ADFR`).


