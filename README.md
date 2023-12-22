# Anonymize DPDash Formatted Data

This script anonymizes DPDash formatted data

## Usage

Confifure behaviour in `config.ini` and run the following scripts in order:

```bash
cd $REPO_ROOT/anonymizer

./scripts/1_generate_site_maps.py
./scripts/2_generate_subject_maps.py
./scripts/3_consolidate_dates.py

./scripts/5_anonymize.py
```
