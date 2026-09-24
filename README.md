# aind-vr-foraging-primary-data-aggregator

Capsule that aggregates session-wise processed data for the VR Foraging task. The packaging and processing is done through this [library](https://github.com/AllenNeuralDynamics/Aind.Behavior.VrForaging.Packaging). This capsule then collects the processed session using queries on the `aind-data-schema` metadata, and then reads the sessions from S3 and aggregates them to parquet files. The two aggregated files are the session and sites parquet files. These are written to the `/results/` folder along with the `data_description` and `processing` `aind-data-schema` metadata files

The capsule accepts these optional run parameters, which can be configured as
named App Panel parameters in Code Ocean:

```text
--selection-mode manifest --manifest-path /data/target.csv
--selection-mode all
--selection-mode all --dry-run
--selection-mode all --min-packaging-version 0.20.0 --max-packaging-version 0.21.0
```

`manifest` is the default and validates the CSV's `session` column against the
selected raw assets. `all` does not use a CSV. It selects the highest packaging
version available for each raw asset, resolving version ties by the newest
creation time.

Add `--dry-run` to write only metadata. It skips Parquet reads and produces
`data_description.json` and `processing.json` (plus the input CSV in manifest
mode).

The packaging-version fields are optional inclusive bounds. When left blank,
both modes are unbounded.

## Scheduled runs (GitHub Actions)

[.github/workflows/run-capsule.yml](.github/workflows/run-capsule.yml) runs
this capsule on Code Ocean every day (13:00 UTC, or on demand through
`workflow_dispatch`). It waits for the run to finish and then publishes the
result as the `vr-foraging-dataset` data asset in
`s3://aind-scratch-data/vr-foraging/vr-foraging-dataset`.

The driver, [.github/run_capsule.py](.github/run_capsule.py), is a single-file
[PEP 723](https://peps.python.org/pep-0723/) script, so `uv run` resolves its
dependencies and no project setup is needed. It reads the Code Ocean API token
from `CODEOCEAN_TOKEN` (a repository secret in CI) or from a gitignored
`secrets/codeocean` file at the repository root when you run it locally:

```bash
uv run .github/run_capsule.py --list-app-panel-parameters   # live App Panel schema
uv run .github/run_capsule.py --list-custom-metadata-fields # asset metadata schema
uv run .github/run_capsule.py --skip-asset-creation          # run only, don't publish
uv run .github/run_capsule.py \
    --name vr-foraging-dataset --mount vr-foraging-dataset \
    --bucket aind-scratch-data --prefix vr-foraging/vr-foraging-dataset \
    --wait-for-asset
```

Capsule parameters are passed by their App Panel display label, which is
resolved to the real `param_name` at runtime. If a panel field is renamed,
update `PARAMETER_DISPLAY_NAMES` in the script. The script won't publish an
asset unless the run succeeded, meaning a successful `end_status`, a zero
`exit_code`, and results present.
