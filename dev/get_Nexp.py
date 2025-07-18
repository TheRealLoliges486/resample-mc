import pandas as pd
import glob
import os
import pyarrow.parquet as pq


# Do I use this also for the systematic variations?? => I guess so. Maybe ask AT?

parquet_dir = "/work/niharrin/tests/resample_mc/src_files/GluGluHtoGG_M-125_2023postBPix/nominal"
parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
columns_to_load = ["mass", "weight", "genWeight"]
df = pd.concat((pd.read_parquet(f, columns=columns_to_load) for f in parquet_files), ignore_index=True)

## Read the sum of the weights from metadata without any systematic variation
sum_weight_central = 0.0
for i in range(len(parquet_files)):
    sum_weight_central += float(pq.read_table(parquet_files[i]).schema.metadata[b'sum_weight_central'])

## Compute the expected number of events
## This is scaled to the full Run3 lumi and the total production XS (=ggH+VBF+VH+ttH+bbH); Taken from https://twiki.cern.ch/twiki/bin/view/LHCPhysics/CERNYellowReportPageAt13TeV
exp = sum(df["weight"]/sum_weight_central) * 55.65 * 0.2270/100 * 1000 * 27.3

with open("Nexp.txt", "w") as f:
    f.write(f"{exp}\n")