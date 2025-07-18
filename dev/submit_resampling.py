import os
import glob
import argparse
import subprocess
import random

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
        execute_command([f"xrdfs root://t3dcachedb.psi.ch:1094/ mkdir -p {os.path.realpath(folder)}"], shell=True)
    else:
        os.makedirs(folder, exist_ok=True)

def submit_slurm_jobs(directory, suffix=""):
    for file in os.listdir(directory):
        if file.endswith(f"{suffix}.sh"):
            os.system(f"sbatch {os.path.join(directory, file)}")

def create_slurm_script(script_path, input_path, output_path, nreplicas, time="01:00:00", partition="short", memory="12G"):
    job_name = f"Rs{script_path.split('/')[-3]}"
        
    with open(script_path, "w") as script_file:
        script_file.write("#!/bin/bash\n")
        # Random delay between 1 and 20 seconds
        rand_int = random.randint(1, 20)
        script_file.write(f"#SBATCH --begin=now+{rand_int}seconds\n")
        script_file.write(f"#SBATCH --requeue\n")
        script_file.write(f"#SBATCH --job-name={job_name}\n")
        script_file.write(f"#SBATCH --output={os.path.join('/'.join(script_path.split('/')[:-1]), 'output_%A_%a.out')}\n")
        script_file.write(f"#SBATCH --error={os.path.join('/'.join(script_path.split('/')[:-1]), 'output_%A_%a.err')}\n")
        script_file.write(f"#SBATCH --time={time}\n")
        script_file.write(f"#SBATCH --partition={partition}\n")
        script_file.write(f"#SBATCH --mem={memory}\n")
        script_file.write(f"#SBATCH --array=0-{int(nreplicas)-1} \n")
        script_file.write("\n")
        
        script_file.write("echo \"Running job $SLURM_ARRAY_TASK_ID\"\n")
        script_file.write("echo \"Array Job ID: $SLURM_ARRAY_JOB_ID\"\n")
        script_file.write("echo \"Array Task ID: $SLURM_ARRAY_TASK_ID\"\n")
        
        script_file.write("cd /work/niharrin/tests/resample_mc/dev/\n")
        if "/pnfs" in os.path.realpath(output_path):
            script_file.write("export TARGET_PATH=/scratch/$USER/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}/output\n")
            script_file.write("mkdir -p $TARGET_PATH\n")
            script_file.write(f"xrdfs root://t3dcachedb.psi.ch:1094// mkdir -p {output_path}\n")
            script_file.write("\n")
            script_file.write("python resamplingMC.py ${SLURM_ARRAY_TASK_ID}"+f" {input_path} $TARGET_PATH\n")
            script_file.write("\n")
            script_file.write(f"xrdcp -fr $TARGET_PATH/* root://t3dcachedb.psi.ch:1094//{output_path}\n")
            script_file.write("rm -rf /scratch/$USER/${SLURM_ARRAY_TASK_ID}\n")
        else:
            script_file.write(f"mkdir -p {output_path}\n")
            script_file.write("python resamplingMC.py ${SLURM_ARRAY_TASK_ID}"+f" {input_path} {output_path}\n")

def main():
    """Main function to submit the resampling procedure for the blinded EFT analysis to SLURM on the PSI Tier 3."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Submit the resampling procedure for the blinded EFT analysis to SLURM on the PSI Tier 3.")
    parser.add_argument("--input", "-i", required=True, help="Input folder leading to the unmerged MC parquet files")
    parser.add_argument("--output", "-o", required=True, help="Output folder for the resampled parquet files (can be on the PNFS)")
    parser.add_argument("--slurm-output", required=False, default="", help="Output folder for the SLURM logs (default: current directory)")
    parser.add_argument("--time", required=False, default="01:00:00", help="Time limit for each job (default: 01:00:00)")
    parser.add_argument("--partition", required=False, default="short", help="Partition to submit the jobs to (default: short)")
    parser.add_argument("--memory", required=False, default="12G", help="Memory limit for each job (default: 12G)")
    parser.add_argument("--nreplicas", "-n", required=True, help="Number of replicas to generate")
    parser.add_argument("--dry", action="store_true", help="Does not submit the SLURM jobs. Only creating the scripts")
    args = parser.parse_args()

    input_folder = os.path.realpath(args.input)
    output_folder = os.path.realpath(args.output)
    slurm_output_folder = os.path.realpath(args.slurm_output)
    nreplicas = args.nreplicas
    time = args.time
    partition = args.partition
    memory = args.memory
    
    procs_folders = [f for f in glob.glob(os.path.join(input_folder, "*")) if os.path.isdir(f)]
    
    # Create the replica output folder
    for i in range(int(nreplicas)): 
        replica_folder = os.path.join(output_folder, f"replica_{i}")
        create_folder(replica_folder)
    
    for procs_folder in procs_folders:
        
        syst_folders = [f for f in glob.glob(os.path.join(procs_folder, "*")) if os.path.isdir(f)]
        
        if len(syst_folders) == 0:
            print(f"No systematics folders found in {procs_folder}. Skipping.")
            continue
        
        current_proc = procs_folder.split("/")[-1]
        
        for syst_folder in syst_folders:
            current_syst = syst_folder.split("/")[-1]
        
            current_slurm_script_folder = os.path.join(slurm_output_folder, current_proc, current_syst)
            current_slurm_script = os.path.join(current_slurm_script_folder, "submit_resampling.sh")
            create_folder(current_slurm_script_folder)
            
            # Create output paths
            current_output_folder = os.path.join(output_folder, "replica_${SLURM_ARRAY_TASK_ID}", current_proc, current_syst)
            current_slurm_script_folder = os.path.join(slurm_output_folder, current_proc, current_syst)
                        
            create_slurm_script(script_path=current_slurm_script, input_path=syst_folder, output_path=current_output_folder, nreplicas=nreplicas, time=time, partition=partition, memory=memory)
            
            if not args.dry:
                print(f"Submitting SLURM job for {current_proc} - {current_syst}")
                execute_command(["sbatch", current_slurm_script])
            # else:
            #     print(f"Dry run: SLURM job script created at {current_slurm_script}. Not submitting.")

    

if __name__ == "__main__":
    main()
