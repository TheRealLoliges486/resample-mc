import numpy as np
import pandas as pd
import glob
import os
import pyarrow.parquet as pq
from scipy.stats import poisson
import pyarrow as pa
import sys
import subprocess

def execute_command(command, return_output=False, shell=False):
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True, shell=shell, env=os.environ)
        print("Script output:", result.stdout)
        print("Script executed successfully.")
        if return_output:
            return (result.stdout).split("\n")[0]
    except subprocess.CalledProcessError as e:
        print("Error executing script:", e.stderr)

def create_folder(folder):
    if "/pnfs" in os.path.realpath(folder):
        execute_command([f"xrdfs root://t3dcachedb.psi.ch:1094/ mkdir -p {os.realpath(folder)}"], shell=True)
    else:
        os.makedirs(folder, exist_ok=True)

# ====================
# CONFIG / ARGUMENTS
# ====================

if len(sys.argv) != 4:
    print(f"Usage: {sys.argv[0]} <replica_idx> <parquet_dir> <output_dir>")
    sys.exit(1)

replica_idx = int(sys.argv[1])
parquet_dir = sys.argv[2]
output_dir = sys.argv[3]

print(f"Replica index: {replica_idx}")
print(f"Parquet input dir: {parquet_dir}")
print(f"Output base dir: {output_dir}")


# Make sure base folder exists
os.makedirs(output_dir, exist_ok=True)


# Load all Parquet files from the specified directory
parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))

df = pd.concat((pd.read_parquet(f) for f in parquet_files), ignore_index=True)

# Extract the sum_genw_presel from the metadata of the Parquet files
# Inpossible to keep it after the "poissonian randomization"
# TODO: Can we always use the same, since in the end we only care about the best fit value?
sum_genw_beforesel = 0
for f in parquet_files:
    sum_genw_beforesel += float(pq.read_table(f).schema.metadata[b'sum_genw_presel'])

## Extract from a Poisson distribution the number of events for each replica
exp_replica = poisson.rvs(mu=len(df), size=(1))


## Indeces corresponding to the events to pick up in each replica
## NB! replace MUST be True, otherwise the sampling is not independent anymore and it is no longer a Poisson process
idx_replica = [np.random.choice(np.array(df.index), replace=True, size=(exp_replica[0]))]

## Extract the events for each replica
replica_df = df.loc[idx_replica[0]]


# Number of events per chunk
chunk_size = 10000

# Split into chunks
num_rows = len(replica_df)
num_chunks = len(range(0, num_rows, chunk_size))
for chunk_idx in range(0, num_rows, chunk_size):
    chunk_df = replica_df.iloc[chunk_idx : chunk_idx + chunk_size]

    # Compute sum of weights
    sum_weight_central = chunk_df["weight"].sum()

    # Convert to pyarrow Table
    table = pa.Table.from_pandas(chunk_df)

    # Add custom metadata
    existing_meta = table.schema.metadata or {}
    new_meta = dict(existing_meta)
    new_meta[b"sum_weight_central"] = str(sum_weight_central).encode()
    
    # IMPORTANT: Keep all chunks. Otherwise the sum_genw_presel will not be correct
    new_meta[b"sum_genw_presel"] = str(sum_genw_beforesel / num_chunks).encode()

    # Write with updated schema
    table = table.replace_schema_metadata(new_meta)


    # Build chunk filename
    chunk_file = os.path.join(output_dir, f"chunk_{chunk_idx // chunk_size}.parquet")

    # Write using pyarrow
    pq.write_table(table, chunk_file)

print(f"The replica Nr. {replica_idx} chunked and saved as Parquet to the folder {output_dir}.")