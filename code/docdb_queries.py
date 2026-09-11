""" DocDB queries for locating VR foraging derived assets """

from datetime import datetime, timezone
from typing import Optional

from aind_data_access_api.document_db import MetadataDbClient

DOCDB_HOST = "api.allenneuraldynamics.org"
DOCDB_DATABASE = "metadata_index"
DOCDB_COLLECTION = "data_assets"
# v2 serves the aind-data-schema v2 records the packaging pipeline writes.
# MetadataDbClient defaults to v1, where these queries return nothing.
DOCDB_VERSION = "v2"

# The data process the VR foraging packaging pipeline writes into
# processing.json, which carries packaging_version in output_parameters.
PACKAGING_PROCESS_NAME = "primary-nwb-packaging-vr-foraging"


def _creation_time(record: dict) -> datetime:
    """Read data_description.creation_time as a tz-aware datetime.

    Records missing or carrying an unparseable creation_time sort oldest.
    """
    data_description = record.get("data_description") or {}
    value = data_description.get("creation_time")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)
    else:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _source_sessions(record: dict) -> tuple[str, ...]:
    """Raw session names this derived asset was built from."""
    data_description = record.get("data_description") or {}
    return tuple(data_description.get("source_data") or ())


def _packaging_version(record: dict) -> Optional[str]:
    """Pull packaging_version out of the packaging data process, if present."""
    processing = record.get("processing") or {}
    for process in processing.get("data_processes") or []:
        if process.get("name") != PACKAGING_PROCESS_NAME:
            continue
        output_parameters = process.get("output_parameters") or {}
        version = output_parameters.get("packaging_version")
        if version is not None:
            return version
    return None


def query_derived_assets_by_packaging_version(
    subject_id: str,
    min_version: Optional[str] = None,
    max_version: Optional[str] = None,
    most_recent_per_session: bool = True,
    client: Optional[MetadataDbClient] = None,
    host: str = DOCDB_HOST,
    database: str = DOCDB_DATABASE,
    collection: str = DOCDB_COLLECTION,
    version: str = DOCDB_VERSION,
) -> list[dict]:
    """Find derived assets packaged by a packaging version in [min, max].

    Parameters
    ----------
    subject_id : str
      Subject whose derived assets to return, e.g. ``"754582"``.
    min_version : Optional[str]
      Inclusive lower bound on packaging_version, e.g. ``"0.0.15"``. None
      leaves the range open below.
    max_version : Optional[str]
      Inclusive upper bound on packaging_version, e.g. ``"0.0.19"``. None
      leaves the range open above.
    most_recent_per_session : bool
      When True (the default) return one asset per source session: the one
      with the most recent data_description.creation_time. Sessions are
      grouped by data_description.source_data. Set False to get every
      match, including superseded repackagings.
    client : Optional[MetadataDbClient]
      Reuse an existing client instead of opening one.
    host, database, collection, version : str
      DocDB connection details, used only when ``client`` is None.

    Returns
    -------
    list[dict]
      One dict per asset with ``asset_name``, ``s3_location``, the
      ``packaging_version`` that matched, ``creation_time`` and the
      ``session_names`` it was derived from, ordered newest first.

    Notes
    -----
    Version bounds are compared as strings, so they hold only while the
    bounds and the stored versions share a digit width: ``"0.0.9"`` sorts
    above ``"0.0.19"``.
    """
    filter_query: dict = {
        "subject.subject_id": subject_id,
        "data_description.data_level": "derived",
        "processing.data_processes": {
            "$elemMatch": {
                "name": PACKAGING_PROCESS_NAME,
                "output_parameters.packaging_version": {"$exists": True},
            }
        },
    }

    projection = {
        "name": 1,
        "location": 1,
        "data_description.creation_time": 1,
        "data_description.source_data": 1,
        "processing.data_processes": 1,
    }

    owns_client = client is None
    if owns_client:
        client = MetadataDbClient(
            host=host,
            database=database,
            collection=collection,
            version=version,
        )

    records = client.retrieve_docdb_records(
        filter_query=filter_query, projection=projection
    )

    results = []
    for record in records:
        version = _packaging_version(record)
        if version is None:
            continue
        if min_version is not None and version < min_version:
            continue
        if max_version is not None and version > max_version:
            continue
        sessions = _source_sessions(record)
        results.append(
            {
                "asset_name": record.get("name"),
                "s3_location": record.get("location"),
                "packaging_version": version,
                "creation_time": _creation_time(record),
                "session_names": list(sessions),
                # Assets with no source_data group under their own name
                # rather than collapsing together.
                "_group": sessions or (record.get("name"),),
            }
        )

    results.sort(key=lambda result: result["creation_time"], reverse=True)
    if most_recent_per_session:
        newest: dict = {}
        for result in results:
            newest.setdefault(result["_group"], result)
        results = list(newest.values())
    for result in results:
        del result["_group"]
    return results
