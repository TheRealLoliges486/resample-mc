import uproot
import ROOT
import numpy as np
from scipy.stats import poisson
import pandas as pd
import mplhep as hep
import glob
import os
import pyarrow.parquet as pq
from scipy.interpolate import CubicSpline
import matplotlib.pyplot as plt
plt.style.use(hep.style.CMS)

try:
    with open("Nexp.txt", "r") as f:
        exp = float(f.read().strip())
except FileNotFoundError:
    print("Nexp.txt not found. Please run get_Nexp.py first to generate it.")
    exit(1)

print(f"Expected number of events in all MC: {exp}")

# Function to evaluate the error bars on data (68% of confidence interval of Poisson distribution)
def poisson_interval(k, alpha=0.317): 
    """
    uses chisquared info to get the poisson interval. Uses scipy.stats 
    (imports in function). 
    """
    from scipy.stats import chi2
    a = alpha
    low, high = (chi2.ppf(a/2, 2*k) / 2, chi2.ppf(1-a/2, 2*k + 2) / 2)
#     if k == 0: 
#         low = 0.0
    return k-low, high-k

# Diese Funktion PRO MC sample ausführen
parquet_dir = "/work/niharrin/tests/resample_mc/src_files/GluGluHtoGG_M-125_2023postBPix/nominal"
parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
columns_to_load = ["mass", "weight", "genWeight"]
# df = pd.concat((pd.read_parquet(f) for f in parquet_files), ignore_index=True)
df = pd.concat((pd.read_parquet(f, columns=columns_to_load) for f in parquet_files), ignore_index=True)

## Read the sum of the weights from metadata without any systematic variation
sum_weight_central = 0.0
for i in range(len(parquet_files)):
    sum_weight_central += float(pq.read_table(parquet_files[i]).schema.metadata[b'sum_weight_central'])

n_replicas = 10000
## Extract from a Poisson distribution the number of events for each replica
exp_replicas = poisson.rvs(mu=exp, size=(len(df)))

df["weight_norm"] = df["weight"] / sum_weight_central
## Probability should be normalised to one
df["prob"] = df["weight_norm"] / sum(df["weight_norm"])

## Indeces corresponding to the events to pick up in each replica
## NB! replace MUST be True, otherwise the sampling is not independent anymore and it is no longer a Poisson process
idx_replicas = [np.random.choice(np.array(df.index), replace=True, size=(exp_replicas[i]), p=df["prob"]) for i in range(n_replicas)]

## Extract the events for each replica
replicas = [df.loc[idx_replicas[i]] for i in range(n_replicas)]








# Base output folder
output_dir = "replica_chunks"

# Make sure base folder exists
os.makedirs(output_dir, exist_ok=True)

# Number of events per chunk
chunk_size = 1000

for replica_idx, replica_df in enumerate(replicas):
    # Create a subfolder for this replica
    replica_folder = os.path.join(output_dir, f"replica_{replica_idx}")
    os.makedirs(replica_folder, exist_ok=True)

    # Split into chunks
    num_rows = len(replica_df)
    for chunk_idx in range(0, num_rows, chunk_size):
        chunk_df = replica_df.iloc[chunk_idx : chunk_idx + chunk_size]

        # Build chunk filename
        chunk_file = os.path.join(replica_folder, f"chunk_{chunk_idx // chunk_size}.parquet")

        # Write chunk to Parquet
        chunk_df.to_parquet(chunk_file, index=False)

print("All replicas chunked and saved as Parquet.")