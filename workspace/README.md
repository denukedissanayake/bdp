# Workspace Directory

This directory is mounted into the Spark container for running Spark jobs.

Place your Spark scripts and data files here to access them from within the container.

## Usage

From within the Spark container:
```bash
docker exec -it spark bash
cd /workspace
spark-submit your_script.py
```
