# Resampling MC

The `submit_resampling.py` script is used to produce the replicas via SLURM. For example launch the production of 10 replicas using the command:

```bash
python submit_resampling.py --input /pnfs/psi.ch/cms/trivcat/store/user/niharrin/ntuples/midRun3/samples/2025_07_17_powheg/src_files/ --output /pnfs/psi.ch/cms/trivcat/store/user/niharrin/ntuples/midRun3/samples/2025_07_17_powheg/replicas/ --slurm-output /t3home/niharrin/devel/work/tests/resample_mc/dev/slurm_output -n 10
```


