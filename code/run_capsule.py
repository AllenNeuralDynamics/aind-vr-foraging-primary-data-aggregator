"""Aggregate common Parquet assets from multiple S3 locations."""

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
import pyarrow.parquet as pq
import s3fs
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
from docdb_queries import DEFAULT_MANIFEST_PATH, query_manifest_derived_assets

logger = logging.getLogger(__name__)

# Set either bound to restrict which packaging versions may be selected.
MIN_PACKAGING_VERSION: str | None = None
MAX_PACKAGING_VERSION: str | None = None
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


def _read_asset(
    filesystem: s3fs.S3FileSystem,
    s3_location: str,
    asset_name: str,
) -> pa.Table:
    """Read one expected Parquet asset and add session provenance when needed."""
    s3_path = _s3_asset_path(s3_location, asset_name)
    try:
        with filesystem.open(s3_path, "rb") as source:
            table = pq.read_table(source)
    except FileNotFoundError as error:
        raise FileNotFoundError(f"Expected {asset_name} at {s3_path}") from error

    if asset_name == SESSION_TABLE:
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
) -> dict[str, Path]:
    """Aggregate the same Parquet assets from every S3 location.

    Each requested asset must exist beneath every supplied S3 prefix. Reads for
    an asset run concurrently (up to ``max_workers``). Rows originating from
    ``session.parquet`` retain their source prefix in ``source_s3_location``.
    """
    if not s3_locations:
        raise ValueError("At least one S3 location is required")
    if not tables:
        raise ValueError("At least one Parquet asset is required")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    # Derived assets are read from the public aind-open-data bucket. Using the
    # capsule IAM role can turn a public object read into a denied signed call.
    filesystem = s3fs.S3FileSystem(anon=True)
    aggregated: dict[str, pa.Table] = {}
    worker_count = min(max_workers, len(s3_locations))

    for asset_name in tables:
        if not asset_name.endswith(".parquet"):
            raise ValueError(f"Expected a Parquet filename, got {asset_name!r}")

        logger.info("Reading %s from %d S3 locations", asset_name, len(s3_locations))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            source_tables = list(
                executor.map(
                    lambda location, asset_name=asset_name: _read_asset(
                        filesystem, location, asset_name
                    ),
                    s3_locations,
                )
            )
        aggregated[asset_name] = pa.concat_tables(
            source_tables,
            promote_options="permissive",
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, Path] = {}
    for asset_name, table in aggregated.items():
        destination = output_dir / asset_name
        pq.write_table(table, destination)
        outputs[asset_name] = destination
        logger.info("Wrote %s rows to %s", table.num_rows, destination)

    return outputs


def _selected_s3_locations() -> list[str]:
    """Get the validated, latest derived-asset locations from DocDB."""
    selected_assets = query_manifest_derived_assets(
        min_version=MIN_PACKAGING_VERSION,
        max_version=MAX_PACKAGING_VERSION,
    )

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
        "Selected %d manifest-validated S3 locations from DocDB", len(locations)
    )
    return locations


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
            "commit": "none",
            "dirty": "none",
            "working_tree_status": "none",
            "status": "none",
        }
    commit = _git_output("rev-parse", "HEAD") or "none"
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
    process = DataProcess(
        process_type=ProcessName.ANALYSIS,
        name="VR foraging primary-data aggregation",
        stage=ProcessStage.ANALYSIS,
        code=Code(
            url=_repository_url(),
            name="aind-vr-foraging-primary-data-aggregator",
            commit_hash=(
                repository["commit"] if isinstance(repository["commit"], str) else None
            ),
            language="Python",
            language_version=f"{sys.version_info.major}.{sys.version_info.minor}",
            input_data=input_assets,
            parameters={
                "packaging_version_bounds": {
                    "minimum": MIN_PACKAGING_VERSION,
                    "maximum": MAX_PACKAGING_VERSION,
                },
                "input_manifest": {
                    "source_path": str(DEFAULT_MANIFEST_PATH),
                    "copied_output_path": DEFAULT_MANIFEST_PATH.name,
                },
                "repository": repository,
            },
        ),
        experimenters=maintainers,
        start_date_time=started_at,
        end_date_time=completed_at,
        output_path=".",
        output_parameters={"outputs": output_entries},
        notes=(
            "Each input source_data value was validated as a one-to-one match "
            "with the VR-paper manifest before aggregation."
        ),
    )
    processing = Processing(data_processes=[process])
    processing.write_standard_file(output_directory=output_dir)
    return output_dir / "processing.json"


def _copy_input_manifest(output_dir: Path) -> Path:
    """Copy the exact CSV used for DocDB selection into the output directory."""
    destination = output_dir / DEFAULT_MANIFEST_PATH.name
    shutil.copy2(DEFAULT_MANIFEST_PATH, destination)
    return destination


def _write_data_description(
    *,
    output_dir: Path,
    creation_time: datetime,
    s3_locations: list[str],
    source_data_description: DataDescription,
) -> Path:
    """Write derived-data metadata for the multi-input aggregate."""
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
        data_summary=(
            "Aggregate of session and site tables from VR foraging primary-data "
            "assets selected from the VR-paper manifest."
        ),
    )
    data_description.write_standard_file(output_directory=output_dir)
    return output_dir / "data_description.json"


def run() -> None:
    """Run the Code Ocean capsule entry point."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    started_at = datetime.now(UTC)
    maintainers = sample(MAINTAINERS, k=len(MAINTAINERS))
    s3_locations = _selected_s3_locations()
    source_data_description = _read_source_data_description(s3_locations)
    outputs = aggregate(s3_locations)
    outputs[DEFAULT_MANIFEST_PATH.name] = _copy_input_manifest(RESULTS_DIR)
    completed_at = datetime.now(UTC)
    outputs["data_description.json"] = _write_data_description(
        output_dir=RESULTS_DIR,
        creation_time=completed_at,
        s3_locations=s3_locations,
        source_data_description=source_data_description,
    )
    processing_path = _write_processing_metadata(
        output_dir=RESULTS_DIR,
        started_at=started_at,
        completed_at=completed_at,
        maintainers=maintainers,
        s3_locations=s3_locations,
        outputs=outputs,
    )
    logger.info("Wrote processing metadata to %s", processing_path)


if __name__ == "__main__":
    run()
