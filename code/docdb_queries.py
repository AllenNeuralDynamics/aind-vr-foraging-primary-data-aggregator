"""DocDB queries for locating VR-foraging derived assets."""

from __future__ import annotations

import csv
import logging
from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from aind_data_access_api.document_db import MetadataDbClient
from packaging.version import InvalidVersion, Version

logger = logging.getLogger(__name__)

DOCDB_HOST = "api.allenneuraldynamics.org"
DOCDB_DATABASE = "metadata_index"
DOCDB_COLLECTION = "data_assets"
DOCDB_VERSION = "v2"
PACKAGING_PROCESS_NAME = "primary-nwb-packaging-vr-foraging"
DEFAULT_MANIFEST_PATH = Path(__file__).with_name("vr_paper_manifest.csv")


def _creation_time(record: dict) -> datetime:
    """Read data_description.creation_time as a timezone-aware datetime."""
    value = (record.get("data_description") or {}).get("creation_time")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return datetime.min.replace(tzinfo=UTC)
    else:
        return datetime.min.replace(tzinfo=UTC)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _source_sessions(record: dict) -> tuple[str, ...]:
    """Return the raw session names from ``data_description.source_data``."""
    source_data = (record.get("data_description") or {}).get("source_data") or ()
    if isinstance(source_data, str):
        return (source_data,)
    return tuple(str(session) for session in source_data)


def _packaging_version(record: dict) -> str | None:
    """Extract the VR-foraging packaging version from a record, if present."""
    processes = (record.get("processing") or {}).get("data_processes") or ()
    for process in processes:
        if process.get("name") != PACKAGING_PROCESS_NAME:
            continue
        version = (process.get("output_parameters") or {}).get("packaging_version")
        if version is not None:
            return str(version)
    return None


def _parse_version(value: str, *, context: str) -> Version:
    try:
        return Version(value)
    except InvalidVersion as error:
        raise ValueError(f"Invalid packaging version {value!r} in {context}") from error


def _manifest_sessions(manifest_path: Path) -> list[str]:
    """Read unique raw session names from the VR-paper manifest."""
    with manifest_path.open(newline="", encoding="utf-8") as manifest_file:
        reader = csv.DictReader(manifest_file)
        if not reader.fieldnames or "session" not in reader.fieldnames:
            raise ValueError(f"{manifest_path} must contain a 'session' column")
        sessions = [
            row["session"].strip() for row in reader if row.get("session", "").strip()
        ]

    duplicates = sorted(
        session for session, count in Counter(sessions).items() if count > 1
    )
    if duplicates:
        raise ValueError(
            "Manifest has duplicate session values: " + ", ".join(duplicates)
        )
    if not sessions:
        raise ValueError(f"{manifest_path} contains no session values")
    return sessions


def _qualifying_packaging_versions(
    client: MetadataDbClient,
    base_filter: dict,
    minimum: Version | None,
    maximum: Version | None,
) -> list[str]:
    """Return the distinct packaging versions matching ``base_filter`` and bounds.

    A cheap ``$unwind``/``$group`` pass over the matching documents' packaging
    versions, used to narrow the main query to an explicit ``$in`` list before
    the expensive per-document array scan.
    """
    pipeline = [
        {"$match": base_filter},
        {"$unwind": "$processing.data_processes"},
        {"$match": {"processing.data_processes.name": PACKAGING_PROCESS_NAME}},
        {
            "$group": {
                "_id": "$processing.data_processes.output_parameters.packaging_version"
            }
        },
    ]
    distinct_versions = [
        record["_id"]
        for record in client.aggregate_docdb_records(pipeline=pipeline)
        if record.get("_id")
    ]
    qualifying: list[str] = []
    for value in distinct_versions:
        parsed = _parse_version(value, context="DocDB packaging_version")
        if (minimum is not None and parsed < minimum) or (
            maximum is not None and parsed > maximum
        ):
            continue
        qualifying.append(value)
    return qualifying


def query_derived_assets_by_packaging_version(
    min_version: str | None = None,
    max_version: str | None = None,
    source_sessions: Iterable[str] | None = None,
    latest_per_source_session: bool = True,
    client: MetadataDbClient | None = None,
    host: str = DOCDB_HOST,
    database: str = DOCDB_DATABASE,
    collection: str = DOCDB_COLLECTION,
    version: str = DOCDB_VERSION,
) -> list[dict]:
    """Query derived assets once, optionally limited to a set of raw sessions.

    Packaging-version bounds are inclusive and use PEP 440 version ordering.
    When ``latest_per_source_session`` is true, each raw session contributes
    only its highest in-range packaging version; creation time breaks ties.
    """
    minimum = (
        _parse_version(min_version, context="min_version") if min_version else None
    )
    maximum = (
        _parse_version(max_version, context="max_version") if max_version else None
    )
    if minimum is not None and maximum is not None and minimum > maximum:
        raise ValueError("min_version cannot be greater than max_version")

    requested_sessions = tuple(dict.fromkeys(source_sessions or ()))
    if source_sessions is not None and not requested_sessions:
        return []

    filter_query: dict = {
        "data_description.data_level": "derived",
        "processing.data_processes": {
            "$elemMatch": {
                "name": PACKAGING_PROCESS_NAME,
                "output_parameters.packaging_version": {"$exists": True},
            }
        },
    }
    if requested_sessions:
        filter_query["data_description.source_data"] = {"$in": list(requested_sessions)}

    projection = {
        "name": 1,
        "location": 1,
        "data_description.creation_time": 1,
        "data_description.source_data": 1,
        "processing.data_processes": 1,
    }
    if client is None:
        client = MetadataDbClient(
            host=host,
            database=database,
            collection=collection,
            version=version,
        )

    if not requested_sessions and (minimum is not None or maximum is not None):
        qualifying_versions = _qualifying_packaging_versions(
            client, filter_query, minimum, maximum
        )
        if not qualifying_versions:
            return []
        filter_query["processing.data_processes"]["$elemMatch"][
            "output_parameters.packaging_version"
        ] = {"$in": qualifying_versions}

    # ``retrieve_docdb_records`` puts its filter in a GET query string. A
    # manifest-sized ``$in`` list exceeds API-gateway URL limits, so use the
    # aggregate endpoint's POST body for the one bulk manifest query.
    if requested_sessions:
        records = client.aggregate_docdb_records(
            pipeline=[{"$match": filter_query}, {"$project": projection}]
        )
    else:
        logger.info(
            "Querying DocDB for all matching derived VR-foraging assets; "
            "processing.data_processes is not indexed, so this scan "
            "typically takes a few minutes regardless of version bounds"
        )
        records = client.retrieve_docdb_records(
            filter_query=filter_query,
            projection=projection,
        )

    results: list[dict] = []
    for record in records:
        packaging_version = _packaging_version(record)
        if packaging_version is None:
            continue
        parsed_version = _parse_version(
            packaging_version,
            context=f"asset {record.get('name')!r}",
        )
        if (minimum is not None and parsed_version < minimum) or (
            maximum is not None and parsed_version > maximum
        ):
            continue

        sessions = _source_sessions(record)
        results.append(
            {
                "asset_name": record.get("name"),
                "s3_location": record.get("location"),
                "packaging_version": packaging_version,
                "creation_time": _creation_time(record),
                "session_names": list(sessions),
                "_version": parsed_version,
            }
        )

    results.sort(
        key=lambda item: (item["_version"], item["creation_time"]), reverse=True
    )
    if latest_per_source_session:
        latest: dict[tuple[str, ...], dict] = {}
        for result in results:
            group = tuple(result["session_names"]) or (str(result["asset_name"]),)
            latest.setdefault(group, result)
        results = list(latest.values())

    for result in results:
        del result["_version"]
    return results


def query_manifest_derived_assets(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    min_version: str | None = None,
    max_version: str | None = None,
    client: MetadataDbClient | None = None,
) -> list[dict]:
    """Return the latest in-range derived asset for every manifest session.

    The manifest session column and selected DocDB ``source_data`` must be a
    one-to-one match. A result with zero or multiple source sessions is
    rejected because it cannot be mapped unambiguously to one manifest row.
    """
    manifest_sessions = _manifest_sessions(manifest_path)
    results = query_derived_assets_by_packaging_version(
        min_version=min_version,
        max_version=max_version,
        source_sessions=manifest_sessions,
        latest_per_source_session=True,
        client=client,
    )

    returned_sessions: list[str] = []
    ambiguous_assets: list[str] = []
    for result in results:
        source_sessions = result["session_names"]
        if len(source_sessions) != 1:
            ambiguous_assets.append(str(result["asset_name"]))
            continue
        returned_sessions.append(source_sessions[0])

    expected = Counter(manifest_sessions)
    actual = Counter(returned_sessions)
    missing = sorted((expected - actual).elements())
    unexpected = sorted((actual - expected).elements())
    duplicates = sorted(session for session, count in actual.items() if count > 1)
    if ambiguous_assets or missing or unexpected or duplicates:
        problems: list[str] = []
        if ambiguous_assets:
            problems.append(
                "assets with non-singleton source_data: " + ", ".join(ambiguous_assets)
            )
        if missing:
            problems.append(
                "manifest sessions with no selected asset: " + ", ".join(missing)
            )
        if unexpected:
            problems.append(
                "selected sessions absent from manifest: " + ", ".join(unexpected)
            )
        if duplicates:
            problems.append(
                "sessions selected more than once: " + ", ".join(duplicates)
            )
        raise ValueError("Manifest/DocDB source-data mismatch; " + "; ".join(problems))

    result_by_session = {result["session_names"][0]: result for result in results}
    return [result_by_session[session] for session in manifest_sessions]


def query_latest_derived_assets_per_source_data(
    min_version: str | None = None,
    max_version: str | None = None,
    client: MetadataDbClient | None = None,
) -> list[dict]:
    """Select one latest derived asset for each raw source-data value.

    Assets are ranked by packaging version and then creation time. A selected
    asset claims every value in its ``source_data`` list, so older candidates
    with an overlapping raw input are skipped instead of causing a failure.
    """
    candidates = query_derived_assets_by_packaging_version(
        min_version=min_version,
        max_version=max_version,
        latest_per_source_session=False,
        client=client,
    )
    selected: list[dict] = []
    selected_source_data: set[str] = set()
    for candidate in candidates:
        source_data = set(candidate["session_names"])
        if source_data & selected_source_data:
            continue
        selected.append(candidate)
        selected_source_data.update(source_data)
    return selected
