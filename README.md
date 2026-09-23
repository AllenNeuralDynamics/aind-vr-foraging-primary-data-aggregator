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
