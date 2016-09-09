#!/bin/csh

#SBATCH -A {slurm_account}
#SBATCH -p {slurm_partition}
#SBATCH -t 4-12:00:00
#SBATCH -n 1
#SBATCH -o {slurm_log_out}/change_detection_%a.out
#SBATCH -e {slurm_log_error}/change_detection_%a.error
#SBATCH --open-mode=append
#SBATCH --job-name=ccdc
#SBATCH --array=1-5000
#SBATCH --mail-type=ALL
#SBATCH --mail-user={slurm_email}
#SBATCH --mem-per-cpu=24576

sleep `shuf -i 1-120 -n 1`

module load matlab/mcr-R2013a-v81

echo "\n\n********************************************"
echo "* This job runs on ${SLURM_JOB_NODELIST}"
echo "********************************************\n\n"

# Set this to the last number of the array. New variable in Slurm 15.08 (coming soon)
setenv MATLAB_NTASKS 5000
#setenv MATLAB_NTASKS ${SLURM_ARRAY_TASK_MAX}
setenv MATLAB_TASK ${SLURM_ARRAY_TASK_ID}

echo "MATLAB_NTASKS = ${MATLAB_NTASKS}"
echo "MATLAB_TASK = ${MATLAB_TASK}"


# Running MATLAB standalone results in creation of a cache folder in home dir
# This may cause runtime issues. Below is a workaround.
echo "tmpdir is ${LOCAL_SCRATCH}"
setenv MCR_CACHE_ROOT ${LOCAL_SCRATCH}
echo "\n\n********************************************"
echo "* Using local scratch folder ${LOCAL_SCRATCH} on ${SLURM_JOB_NODELIST}"
echo "********************************************\n\n"

# Ready to run standalone with commandline inputs (if required).
# $MATLAB_TASK --- processor number
# $MATLAB_NTASKS --- processor total
#
cd {slurm_job_input_dir}

srun {change_detection_path} ${MATLAB_TASK} ${MATLAB_NTASKS}

exit
