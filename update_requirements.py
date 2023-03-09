# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1: update the requirements.txt file
# MAGIC 
# MAGIC Run this step after you have modified the `requirements.in` file, adding your dependencies there.

# COMMAND ----------

!pip-compile --extra-index-url https://${{JFROG_USERNAME}}:${{JFROG_PASSWORD}}@prima.jfrog.io/artifactory/api/pypi/primapy/simple --no-emit-index-url

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: update the bash script used by the cluster at startup to install the requirements

# COMMAND ----------

from pathlib import Path
import shutil

REPO_NAME = "dask-feature-store"
PROJECT_PATH = Path(f"/dbfs/databricks/init-scripts/{REPO_NAME}")
PROJECT_PATH.mkdir(exist_ok=True, parents=True)

print(f"Going to write init script and requirements inside {PROJECT_PATH}")

requirements_path = shutil.copy("requirements.txt", PROJECT_PATH)

init_script = f"""
#!/bin/bash
# CREATE DIRECTORY ON DBFS FOR LOGS
LOG_DIR=/dbfs/databricks/scripts/logs/$DB_CLUSTER_ID/dask/
HOSTNAME=`hostname`
mkdir -p $LOG_DIR
# INSTALL DASK AND OTHER DEPENDENCIES
set -ex
pip install --extra-index-url https://${{JFROG_USERNAME}}:${{JFROG_PASSWORD}}@prima.jfrog.io/artifactory/api/pypi/primapy/simple -r {requirements_path}

# START DASK – ON DRIVER NODE START THE SCHEDULER PROCESS 
# ON WORKER NODES START WORKER PROCESSES
if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  dask-scheduler &>/dev/null &
  echo $! > $LOG_DIR/dask-scheduler.$HOSTNAME.pid
else
  dask-worker tcp://$DB_DRIVER_IP:8786 --nprocs 4 --nthreads 8 &>/dev/null &
  echo $! > $LOG_DIR/dask-worker.$HOSTNAME.pid
fi
"""

init_script_path = PROJECT_PATH / "startup.sh"
with open(init_script_path, "w") as fp:
    fp.write(init_script)

# COMMAND ----------

print("Set the following init script in your cluster settings: ", str(init_script_path).replace("/dbfs", "dbfs:"))

# COMMAND ----------


