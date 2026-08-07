## Automation folder

This folder contains automation components for managing automation jobs and submitting computation jobs in Argo Monitoring. The main component is `argo_automator`, a daemon that listens for events on AMS and executes triggered jobs.

### Getting started

First, set up a Python virtual environment:
```bash
python -m venv ./argo-venv
source ./argo-venv/bin/activate
pip install -r requirements.txt
```

**Requirements:**
- Python 3.9+
- Dependencies: requests, argo-ams-library, pyyaml, pymongo

### Running the automator daemon

Start the automator with:
```bash
./argo_automator
```

By default it looks for `.config.yml` in the current directory. To specify a different config file:
```bash
./argo_automator -c /path/to/config.yml
```

See `config.yml.example` for configuration details.

### Job submission scripts

#### Ingest job
Submit an ingestion job for a tenant:
```bash
./run_ingest -t TENANTFOO
```

**Options:**
- `-c /path/to/config.yml` - Specify config file (default: `.config.yml`)
- `--performance` - Enable ingestion of performance data. By default is not enabled
- `--no-verify` - Skip verification of remote endpoints like AMS
- `--dry-run` - Preview what would be submitted without executing
- `--log-level DEBUG` - Adjust logging verbosity

#### Batch job
Submit a batch computation job (calculates AR, status, and trends):
```bash
./run_batch -t TENANTFOO -r Default
```

**Options:**
- `-d 2025-05-05` - Specify date (default: current day)
- `-c /path/to/config.yml` - Specify config file
- `--dry-run` - Preview submission
- `--log-level DEBUG` - Adjust logging verbosity