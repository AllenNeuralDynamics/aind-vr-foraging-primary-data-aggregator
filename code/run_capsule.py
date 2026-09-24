"""Aggregate common Parquet assets from multiple S3 locations."""

import argparse
import hashlib
import logging
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from random import sample

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import s3fs
from aind_behavior_vr_foraging_packaging.schema_migrations import (
    SchemaMigrationMode,
    append_migrated_schema_columns,
)
from aind_data_schema.core.data_description import (
    DataDescription,
    DataLevel,
    build_data_name,
)
from aind_data_schema.core.processing import (
    Code,
    DataAsset,
    DataProcess,
    Processing,
    ProcessName,
    ProcessStage,
)
from docdb_queries import (
    DEFAULT_MANIFEST_PATH,
    query_latest_derived_assets_per_source_data,
    query_manifest_derived_assets,
)
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger(__name__)

# aind_behavior_vr_foraging.task_logic logs one WARNING per legacy field it
# silently upgrades (e.g. increment -> on_success) while schema migration
# deserializes a historical document. Every legacy row triggers several of
# these; they are expected noise from a working compatibility shim, not
# something an aggregation run can act on.
logging.getLogger("aind_behavior_vr_foraging.task_logic").setLevel(logging.ERROR)

TABLES_TO_AGGREGATE: tuple[str, ...] = ("session.parquet", "sites.parquet")
SESSION_TABLE = "session.parquet"
SOURCE_LOCATION_COLUMN = "source_s3_location"
REPOSITORY_URL = (
    "https://github.com/AllenNeuralDynamics/aind-vr-foraging-primary-data-aggregator"
)
MAINTAINERS = ("bruno.cruz", "arjun.sridhar")
RESULTS_DIR = (
    Path("/results")
    if os.name != "nt"
    else Path(__file__).resolve().parents[1] / "results"
)


def _s3_asset_path(s3_location: str, asset_name: str) -> str:
    """Build the URI of one asset stored below an S3 prefix."""
    if not s3_location.startswith("s3://"):
        raise ValueError(f"Expected an S3 URI, got {s3_location!r}")
    return f"{s3_location.rstrip('/')}/{asset_name}"


def _normalize_date_timezone(table: pa.Table) -> pa.Table:
    """Represent timestamp values in a top-level ``date`` column as UTC."""
    column_index = table.schema.get_field_index("date")
    if column_index == -1:
        return table

    column = table.column(column_index)
    if not pa.types.is_timestamp(column.type):
        return table

    utc_type = pa.timestamp("us", tz="UTC")
    if column.type.tz is None:
        column = pc.assume_timezone(column, timezone="UTC")
    column = pc.cast(column, utc_type)
    return table.set_column(column_index, "date", column)


def _read_asset(
    filesystem: s3fs.S3FileSystem,
    s3_location: str,
    asset_name: str,
    schema_migration_mode: SchemaMigrationMode = SchemaMigrationMode.FILL_MISSING,
) -> pa.Table:
    """Read one expected Parquet asset and add source provenance when needed."""
    s3_path = _s3_asset_path(s3_location, asset_name)
    try:
        with filesystem.open(s3_path, "rb") as source:
            table = pq.read_table(source)
    except FileNotFoundError as error:
        raise FileNotFoundError(f"Expected {asset_name} at {s3_path}") from error

    table = _normalize_date_timezone(table)

    if asset_name == SESSION_TABLE:
        table = append_migrated_schema_columns(
            table,
            mode=schema_migration_mode,
        )
        if SOURCE_LOCATION_COLUMN in table.column_names:
            raise ValueError(
                f"{s3_path} already contains the reserved {SOURCE_LOCATION_COLUMN!r} column"
            )
        source_locations = pa.array(
            [s3_location] * table.num_rows,
            type=pa.string(),
        )
        table = table.append_column(SOURCE_LOCATION_COLUMN, source_locations)

    # File-level metadata cannot truthfully describe an aggregate of many files.
    return table.replace_schema_metadata(None)


def _read_source_data_description(s3_locations: list[str]) -> DataDescription:
    """Load stable organizational metadata from the first selected input asset."""
    source_path = _s3_asset_path(s3_locations[0], "data_description.json")
    filesystem = s3fs.S3FileSystem(anon=True)
    try:
        with filesystem.open(source_path, "rb") as source:
            return DataDescription.model_validate_json(source.read())
    except (FileNotFoundError, PermissionError, ValueError) as error:
        raise RuntimeError(
            f"Could not load a valid data_description.json from {source_path}"
        ) from error


def aggregate(
    s3_locations: tuple[str, ...] | list[str],
    output_dir: Path = RESULTS_DIR,
    tables: tuple[str, ...] = TABLES_TO_AGGREGATE,
    max_workers: int = 32,
    schema_migration_mode: SchemaMigrationMode = SchemaMigrationMode.FILL_MISSING,
) -> dict[str, Path]:
    """Aggregate the same Parquet assets from every S3 location.

    Each requested asset must exist beneath every supplied S3 prefix. Reads for
    an asset run concurrently (up to ``max_workers``). ``session.parquet`` is
    aggregated first and maps each source prefix to the ``session_id`` added to
    other tables.
    """
    if not s3_locations:
        raise ValueError("At least one S3 location is required")
    if not tables:
        raise ValueError("At least one Parquet asset is required")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if SESSION_TABLE not in tables:
        raise ValueError(f"{SESSION_TABLE} is required for session provenance")

    # Derived assets are read from the public aind-open-data bucket. Using the
    # capsule IAM role can turn a public object read into a denied signed call.
    filesystem = s3fs.S3FileSystem(anon=True)
    aggregated: dict[str, pa.Table] = {}
    worker_count = min(max_workers, len(s3_locations))
    session_by_location: dict[str, str] = {}
    tables = (SESSION_TABLE, *(table for table in tables if table != SESSION_TABLE))

    for asset_name in tables:
        if not asset_name.endswith(".parquet"):
            raise ValueError(f"Expected a Parquet filename, got {asset_name!r}")

        logger.info("Reading %s from %d S3 locations", asset_name, len(s3_locations))
        with logging_redirect_tqdm(), ThreadPoolExecutor(max_workers=worker_count) as executor:
            source_tables = list(
                tqdm(
                    executor.map(
                        lambda location, asset_name=asset_name: _read_asset(
                            filesystem,
                            location,
                            asset_name,
                            schema_migration_mode,
                        ),
                        s3_locations,
                    ),
                    total=len(s3_locations),
                    desc=f"Reading {asset_name}",
                    file=sys.stdout,
                )
            )

        if asset_name != SESSION_TABLE:
            source_tables = [
                table.add_column(
                    0,
                    pa.field("session_id", pa.large_string()),
                    pa.repeat(
                        pa.scalar(
                            session_by_location[location], type=pa.large_string()
                        ),
                        table.num_rows,
                    ),
                )
                if "session_id" not in table.column_names
                else table
                for table, location in zip(source_tables, s3_locations, strict=True)
            ]
        aggregated[asset_name] = pa.concat_tables(
            source_tables,
            promote_options="permissive",
        )
        if asset_name == SESSION_TABLE:
            session_by_location = dict(
                zip(
                    aggregated[asset_name].column(SOURCE_LOCATION_COLUMN).to_pylist(),
                    aggregated[asset_name].column("session_id").to_pylist(),
                    strict=True,
                )
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, Path] = {}
    for asset_name, table in aggregated.items():
        destination = output_dir / asset_name
        pq.write_table(table, destination)
        outputs[asset_name] = destination
        logger.info("Wrote %s rows to %s", table.num_rows, destination)

    return outputs


def _selected_s3_locations(
    selection_mode: str,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    min_version: str | None = None,
    max_version: str | None = None,
) -> list[str]:
    """Get selected derived-asset locations from DocDB."""
    if selection_mode == "manifest":
        selected_assets = query_manifest_derived_assets(
            manifest_path=manifest_path,
            min_version=min_version,
            max_version=max_version,
        )
    elif selection_mode == "all":
        selected_assets = query_latest_derived_assets_per_source_data(
            min_version=min_version,
            max_version=max_version,
        )
    else:
        raise ValueError(f"Unknown selection mode: {selection_mode!r}")

    locations: list[str] = []
    assets_by_location: dict[str, str] = {}
    for asset in selected_assets:
        location = asset.get("s3_location")
        asset_name = str(asset.get("asset_name"))
        if not isinstance(location, str) or not location.startswith("s3://"):
            raise ValueError(
                f"Asset {asset_name!r} has no valid S3 location: {location!r}"
            )
        if location in assets_by_location:
            raise ValueError(
                "DocDB selected the same S3 asset location more than once: "
                f"{location} ({assets_by_location[location]!r}, {asset_name!r})"
            )
        assets_by_location[location] = asset_name
        locations.append(location)

    logger.info(
        "Selected %d S3 locations from DocDB using %s mode",
        len(locations),
        selection_mode,
    )
    return locations


def _selection_version_bounds(
    selection_mode: str,
    min_version: str | None,
    max_version: str | None,
) -> dict[str, str | None]:
    """Return the packaging-version bounds applied by the selection mode."""
    if selection_mode not in {"manifest", "all"}:
        raise ValueError(f"Unknown selection mode: {selection_mode!r}")
    return {"minimum": min_version, "maximum": max_version}


def _parse_bool(value: str) -> bool:
    """Parse the true/false values passed by the Code Ocean App Panel."""
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    raise argparse.ArgumentTypeError("expected 'true' or 'false'")


def _git_output(*arguments: str) -> str | None:
    """Run a Git command at the repository root, if Git metadata is present."""
    try:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=Path(__file__).resolve().parents[1],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        logger.warning("Git metadata is unavailable: %s", error)
        return None
    return completed.stdout.strip()


def _repository_metadata() -> dict[str, object]:
    """Capture the commit and complete working-tree state for this run."""
    status = _git_output("status", "--porcelain=v1", "--untracked-files=all")
    if status is None:
        return {
            "commit": None,
            "dirty": None,
            "working_tree_status": None,
            "status": "unavailable",
        }
    commit = _git_output("rev-parse", "HEAD")
    status_lines = status.splitlines()
    return {
        "commit": commit,
        "dirty": bool(status_lines),
        "working_tree_status": status_lines,
        "status": "available",
    }


def _repository_url() -> str:
    """Return the origin URL in a portable HTTPS form."""
    remote = _git_output("remote", "get-url", "origin")
    if remote is None:
        return REPOSITORY_URL
    if remote.startswith("git@github.com:"):
        remote = f"https://github.com/{remote.removeprefix('git@github.com:')}"
    return remote.removesuffix(".git")


def _sha256(path: Path) -> str:
    """Return the SHA-256 digest of a file without loading it into memory."""
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _write_processing_metadata(
    *,
    output_dir: Path,
    started_at: datetime,
    completed_at: datetime,
    maintainers: list[str],
    s3_locations: list[str],
    outputs: dict[str, Path],
    selection_mode: str,
    packaging_version_bounds: dict[str, str | None],
    manifest_path: Path | None,
    dry_run: bool,
) -> Path:
    """Write the run provenance as a schema-validated processing.json."""
    repository = _repository_metadata()
    input_assets = [
        DataAsset(name=location.rsplit("/", maxsplit=1)[-1], url=location)
        for location in s3_locations
    ]
    output_entries = [
        {"name": name, "path": str(path), "sha256": _sha256(path)}
        for name, path in outputs.items()
    ]
    code_fields: dict[str, object] = {
        "url": _repository_url(),
        "name": "aind-vr-foraging-primary-data-aggregator",
        "language": "Python",
        "language_version": f"{sys.version_info.major}.{sys.version_info.minor}",
        "input_data": input_assets,
        "parameters": {
            "packaging_version_bounds": packaging_version_bounds,
            "selection_mode": selection_mode,
            "dry_run": dry_run,
            "repository": repository,
        },
    }
    # Code Ocean capsules may not contain Git or a .git directory. Omit the
    # field in that case: the schema permits an unknown hash, but not "none".
    if isinstance(repository["commit"], str):
        code_fields["commit_hash"] = repository["commit"]
    if manifest_path is not None:
        code_fields["parameters"]["input_manifest"] = {
            "source_path": str(manifest_path),
            "copied_output_path": manifest_path.name,
        }
    notes = (
        "Each input source_data value was validated as a one-to-one match "
        "with the VR-paper manifest before aggregation."
        if selection_mode == "manifest"
        else "Assets were selected by packaging version and creation time, with "
        "each raw source_data value used by at most one processed asset."
    )
    if dry_run:
        notes += " This was a metadata-only dry run."

    process = DataProcess(
        process_type=ProcessName.ANALYSIS,
        name="VR foraging primary-data aggregation",
        stage=ProcessStage.ANALYSIS,
        code=Code(**code_fields),
        experimenters=maintainers,
        start_date_time=started_at,
        end_date_time=completed_at,
        output_path=".",
        output_parameters={"outputs": output_entries},
        notes=notes,
    )
    processing = Processing(data_processes=[process])
    processing.write_standard_file(output_directory=output_dir)
    return output_dir / "processing.json"


def _copy_input_manifest(manifest_path: Path, output_dir: Path) -> Path:
    """Copy the exact CSV used for DocDB selection into the output directory."""
    destination = output_dir / manifest_path.name
    shutil.copy2(manifest_path, destination)
    return destination


def _write_data_description(
    *,
    output_dir: Path,
    creation_time: datetime,
    s3_locations: list[str],
    source_data_description: DataDescription,
    selection_mode: str,
    dry_run: bool,
) -> Path:
    """Write derived-data metadata for the multi-input aggregate."""
    data_summary = (
        "Aggregate of session and site tables from VR foraging primary-data "
        "assets selected from the VR-paper manifest."
        if selection_mode == "manifest"
        else "Aggregate of session and site tables from all available VR-foraging "
        "packaging outputs."
    )
    if dry_run:
        data_summary = (
            "Metadata-only dry run for selected VR-foraging primary-data assets."
        )
    data_description = DataDescription(
        name=build_data_name("vr-foraging-primary-data-aggregate", creation_time),
        creation_time=creation_time,
        institution=source_data_description.institution,
        funding_source=source_data_description.funding_source,
        data_level=DataLevel.DERIVED,
        investigators=source_data_description.investigators,
        project_name=source_data_description.project_name,
        modalities=source_data_description.modalities,
        license=source_data_description.license,
        tags=source_data_description.tags,
        source_data=[location.rsplit("/", maxsplit=1)[-1] for location in s3_locations],
        data_summary=data_summary,
    )
    data_description.write_standard_file(output_directory=output_dir)
    return output_dir / "data_description.json"


def _parse_arguments() -> argparse.Namespace:
    """Parse Code Ocean App Panel or local command-line parameters."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--selection-mode",
        choices=("manifest", "all"),
        default="manifest",
        help="Use a target CSV manifest or all available assets.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="CSV containing the target session column for manifest mode.",
    )
    parser.add_argument(
        "--min-packaging-version",
        help="Inclusive minimum packaging version; leave blank for no lower bound.",
    )
    parser.add_argument(
        "--max-packaging-version",
        help="Inclusive maximum packaging version; leave blank for no upper bound.",
    )
    parser.add_argument(
        "--dry-run",
        nargs="?",
        const="true",
        default=False,
        type=_parse_bool,
        help="Write metadata without reading or aggregating Parquet files (true/false).",
    )
    parser.add_argument(
        "--schema-migration-mode",
        choices=tuple(SchemaMigrationMode),
        default=SchemaMigrationMode.FILL_MISSING,
        type=SchemaMigrationMode,
        help="Choose disabled, fill-missing, or force handling for migrated schemas.",
    )
    return parser.parse_args()


def run() -> None:
    """Run the Code Ocean capsule entry point."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    started_at = datetime.now(UTC)
    maintainers = sample(MAINTAINERS, k=len(MAINTAINERS))
    arguments = _parse_arguments()
    manifest_path = (
        arguments.manifest_path if arguments.selection_mode == "manifest" else None
    )
    packaging_version_bounds = _selection_version_bounds(
        arguments.selection_mode,
        arguments.min_packaging_version or None,
        arguments.max_packaging_version or None,
    )
    s3_locations = _selected_s3_locations(
        arguments.selection_mode,
        arguments.manifest_path,
        packaging_version_bounds["minimum"],
        packaging_version_bounds["maximum"],
    )
    source_data_description = _read_source_data_description(s3_locations)
    outputs = (
        {}
        if arguments.dry_run
        else aggregate(
            s3_locations,
            schema_migration_mode=arguments.schema_migration_mode,
        )
    )
    if arguments.dry_run:
        logger.info("Dry run: skipped Parquet aggregation")
    if manifest_path is not None:
        outputs[manifest_path.name] = _copy_input_manifest(manifest_path, RESULTS_DIR)
    completed_at = datetime.now(UTC)
    outputs["data_description.json"] = _write_data_description(
        output_dir=RESULTS_DIR,
        creation_time=completed_at,
        s3_locations=s3_locations,
        source_data_description=source_data_description,
        selection_mode=arguments.selection_mode,
        dry_run=arguments.dry_run,
    )
    processing_path = _write_processing_metadata(
        output_dir=RESULTS_DIR,
        started_at=started_at,
        completed_at=completed_at,
        maintainers=maintainers,
        s3_locations=s3_locations,
        outputs=outputs,
        selection_mode=arguments.selection_mode,
        packaging_version_bounds=packaging_version_bounds,
        manifest_path=manifest_path,
        dry_run=arguments.dry_run,
    )
    logger.info("Wrote processing metadata to %s", processing_path)


if __name__ == "__main__":
    run()
