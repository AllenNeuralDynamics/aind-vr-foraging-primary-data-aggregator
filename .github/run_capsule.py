# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "codeocean==0.17.0",
# ]
# ///
"""Trigger this capsule on Code Ocean, wait for it to finish, and publish the
result as a data asset - end to end.

This chains two Code Ocean API calls that are normally done by hand:

    1. computations.run_capsule(...)   - equivalent of pressing "Reproducible
       Run" with the capsule's parameter panel filled in.
    2. data_assets.create_data_asset(...) - equivalent of the "New Result"
       dialog ("Create Result Asset") once the run succeeds.

Capsule parameters are NOT hardcoded as guesses. A capsule's parameter panel
("App Panel") defines a `param_name` per field that is the real key the API
expects - it's rarely identical to the label shown in the UI. This script
resolves each display label (e.g. "Selection mode") to its `param_name` by
querying capsules.get_capsule_app_panel(...) at runtime, so it keeps working
even if the capsule owner renames or reorders fields.

Self-contained PEP 723 script: `uv run` installs its dependencies on the fly,
no pyproject.toml needed. The Code Ocean API token is read from the
CODEOCEAN_TOKEN environment variable, falling back to a gitignored
`secrets/codeocean` file at the repository root.

Usage:
    # See the live App Panel schema for this capsule (display name, the real
    # param_name key, type, default, allowed values).
    uv run .github/run_capsule.py --list-app-panel-parameters

    # See this deployment's custom metadata schema for published assets.
    uv run .github/run_capsule.py --list-custom-metadata-fields

    # Run with the default parameters, then publish the result as a data
    # asset once it completes.
    uv run .github/run_capsule.py \\
        --name vr-foraging-dataset \\
        --mount vr-foraging-dataset \\
        --bucket aind-scratch-data \\
        --prefix vr-foraging/vr-foraging-dataset \\
        --wait-for-asset

    # Publish an already-finished computation without running the capsule.
    uv run .github/run_capsule.py \\
        --computation-id 3f9c1e2a-1234-4d5e-9abc-0123456789ab \\
        --name vr-foraging-dataset \\
        --mount vr-foraging-dataset

    # Only trigger + wait for the run, without publishing anything.
    uv run .github/run_capsule.py --skip-asset-creation
"""

import argparse
import os
import sys
from pathlib import Path

from codeocean import CodeOcean
from codeocean.computation import ComputationEndStatus, NamedRunParam, RunParams
from codeocean.data_asset import AWSS3Target, ComputationSource, DataAssetParams, Source, Target

CODEOCEAN_DOMAIN = "https://codeocean.allenneuraldynamics.org"
_SECRETS_FILE = Path(__file__).resolve().parents[1] / "secrets" / "codeocean"

# https://codeocean.allenneuraldynamics.org/capsule/4688895 shows this numeric
# slug in the browser, but the API rejects it ("400 Bad Request - invalid id")
# - it only accepts the real capsule UUID below (aind-vr-foraging-primary-data-aggregator).
CAPSULE_ID = "1e3252a5-8473-4ea8-ba24-9bada70d4fdd"

# Maps our CLI flag -> the App Panel's display label for that field. The
# label is resolved to its real `param_name` at runtime (see
# resolve_named_parameters) instead of being hardcoded here, since the two
# are usually different strings (e.g. label "Is Dry Run" -> param_name
# "dry-run").
PARAMETER_DISPLAY_NAMES = {
    "selection_mode": "Selection mode",
    "target_csv": "Target CSV",
    "dry_run": "Is Dry Run",
    "min_packaging_version": "Minimum packaging version",
    "max_packaging_version": "Maximum packaging version",
    "schema_migration_mode": "Schema migration mode",
}

# 'Custom Metadata' panel: keys must match the field names printed by
# --list-custom-metadata-fields for this deployment. "modality" and
# "subject id" are left unset here, same as an optional field left blank
# in the UI.
CUSTOM_METADATA = {
    "data level": "derived",
    "experiment type": "behavior",
    "institution": "AIND",
    "subject species": "Mus musculus",
}


def get_codeocean_client() -> CodeOcean:
    """Initialize Code Ocean client.

    Resolves the API token in order:
    1. ``CODEOCEAN_TOKEN`` environment variable
    2. ``secrets/codeocean`` file at the repository root (fallback, gitignored)
    """
    token = os.environ.get("CODEOCEAN_TOKEN")
    if token is None:
        token = _SECRETS_FILE.read_text().strip()

    # The codeocean SDK sends a `Min-Server-Version` header and this
    # deployment's server (4.7.3) rejects ANY request - regardless of ID or
    # endpoint - if that header exceeds its own version. The SDK's default
    # (0.17.0 -> "4.8.0") is newer than what this org runs, so every call
    # would 400 with "server version 4.7.3 is lower than required minimum
    # 4.8.0" otherwise. Lowering it to match the deployment unblocks calls;
    # this is safe to remove once the org's Code Ocean is upgraded past 4.8.0.
    CodeOcean.MIN_SERVER_VERSION = "4.7.3"  # type: ignore[assignment]

    return CodeOcean(domain=CODEOCEAN_DOMAIN, token=token)


def build_data_asset_params(
    *,
    computation_id: str,
    name: str,
    mount: str,
    description: str = "",
    tags: list[str] | None = None,
    bucket: str | None = None,
    prefix: str | None = None,
    custom_metadata: dict | None = None,
) -> DataAssetParams:
    """Build the request body for registering a computation result as a data asset."""
    # 'Destination': omitting `target` stores the asset in Code Ocean's own
    # managed storage ("Code Ocean (Default)" in the UI). Passing a bucket
    # instead selects "External S3 Bucket", writing the files to your bucket.
    target = Target(aws=AWSS3Target(bucket=bucket, prefix=prefix)) if bucket else None

    return DataAssetParams(
        name=name,
        mount=mount,
        tags=tags or [],
        description=description or None,
        # 'Source': the run being converted into a result asset. No path =
        # the entire run output.
        source=Source(computation=ComputationSource(id=computation_id)),
        target=target,
        custom_metadata=custom_metadata or {},
    )


def list_app_panel_parameters(co_client, capsule_id: str) -> None:
    """Print this capsule's live App Panel parameter schema.

    `name` is the UI label; `param_name` is the actual key
    NamedRunParam.param_name must use - they are defined independently by
    the capsule author and are not guaranteed to match.
    """
    panel = co_client.capsules.get_capsule_app_panel(capsule_id)
    for p in panel.parameters or []:
        options = p.value_options if p.value_options else "(free text)"
        print(
            f"  name={p.name!r:32} param_name={p.param_name!r:28} "
            f"value_type={p.value_type!s:8} default={p.default_value!r:10} "
            f"required={bool(p.required)!s:5} options={options}"
        )


def list_custom_metadata_fields(co_client) -> None:
    """Print this deployment's admin-defined custom metadata schema.

    The "Custom Metadata" panel in the UI is not fixed by the SDK - it is
    configured per Code Ocean deployment. The keys in CUSTOM_METADATA must
    exactly match ``field.name`` here.
    """
    schema = co_client.custom_metadata.get_custom_metadata()
    for f in schema.fields or []:
        allowed = f.allowed_values if f.allowed_values else "(free text)"
        print(
            f"  name={f.name!r:30} type={f.type:8} "
            f"required={bool(f.required)!s:5} allowed={allowed}"
        )


def resolve_named_parameters(co_client, capsule_id: str, values: dict) -> list:
    """Turn {cli_flag: value} into [NamedRunParam(...)] using the capsule's
    live App Panel schema to look up each field's real param_name.

    Skips any flag whose value is None (an optional field left blank, same
    as leaving it empty in the UI).
    """
    panel = co_client.capsules.get_capsule_app_panel(capsule_id)
    display_to_param_name = {
        p.name.strip().lower(): p.param_name for p in panel.parameters or [] if p.param_name
    }

    named_params = []
    for cli_flag, value in values.items():
        if value is None:
            continue
        display_name = PARAMETER_DISPLAY_NAMES[cli_flag]
        param_name = display_to_param_name.get(display_name.strip().lower())
        if param_name is None:
            raise SystemExit(
                f"Capsule {capsule_id} has no App Panel parameter named {display_name!r}. "
                "Run --list-app-panel-parameters to see the current schema and update "
                "PARAMETER_DISPLAY_NAMES in this script if the capsule owner renamed it."
            )
        named_params.append(NamedRunParam(param_name=param_name, value=str(value)))
    return named_params


def run_capsule(co_client, args) -> str:
    """Run the capsule, wait for it, and return the computation ID. Exits
    non-zero if the run did not succeed."""
    named_parameters = resolve_named_parameters(
        co_client,
        args.capsule_id,
        {
            "selection_mode": args.selection_mode,
            "target_csv": args.target_csv,
            "dry_run": args.dry_run,
            "min_packaging_version": args.min_packaging_version,
            "max_packaging_version": args.max_packaging_version,
            "schema_migration_mode": args.schema_migration_mode,
        },
    )

    print(f"Starting capsule {args.capsule_id} with parameters:")
    for p in named_parameters:
        print(f"  {p.param_name}={p.value}")

    computation = co_client.computations.run_capsule(
        RunParams(capsule_id=args.capsule_id, named_parameters=named_parameters)
    )
    print(f"Submitted computation: id={computation.id} state={computation.state}")

    print(f"Waiting for the run to finish (polling every {args.poll_interval}s)...")
    computation = co_client.computations.wait_until_completed(
        computation, polling_interval=max(args.poll_interval, 5), timeout=args.timeout
    )
    print(f"Run finished: state={computation.state} end_status={computation.end_status}")

    # end_status alone is not trustworthy: Code Ocean has been observed reporting
    # "succeeded" for a computation whose script actually crashed (nonzero exit_code,
    # has_results=False) - relying on end_status alone let a failed run get silently
    # "published" from stale results already sitting at the target location.
    run_failed = (
        computation.end_status != ComputationEndStatus.Succeeded
        or computation.exit_code not in (0, None)
        or computation.has_results is False
    )
    if run_failed:
        print(
            f"Run did not succeed (end_status={computation.end_status}, "
            f"exit_code={computation.exit_code}, has_results={computation.has_results}) "
            "- not publishing a result asset.",
            file=sys.stderr,
        )
        sys.exit(1)

    return computation.id


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--capsule-id",
        default=CAPSULE_ID,
        help=f"Capsule to run. Defaults to {CAPSULE_ID!r} (this capsule).",
    )
    parser.add_argument(
        "--computation-id",
        default=None,
        help="Publish this already-finished computation instead of running the capsule. "
        "This is the computation UUID, NOT the short numeric run number shown in the UI.",
    )

    # --- Capsule App Panel parameters (exposed 1:1) ---
    parser.add_argument("--selection-mode", default="all", help="'Selection mode' panel field.")
    parser.add_argument(
        "--target-csv",
        default=None,
        help="'Target CSV' panel field. Optional - omit to leave it blank, as in the UI.",
    )
    parser.add_argument(
        "--dry-run",
        choices=["True", "False"],
        default="False",
        help="'Is Dry Run' panel field - the capsule's OWN dry-run flag. This still submits "
        "a real computation to Code Ocean; it just tells the capsule to write metadata only.",
    )
    parser.add_argument(
        "--min-packaging-version",
        default="0.20.0",
        help="'Minimum packaging version' panel field.",
    )
    parser.add_argument(
        "--max-packaging-version",
        default="0.23.0",
        help="'Maximum packaging version' panel field.",
    )
    parser.add_argument(
        "--schema-migration-mode",
        choices=["disabled", "fill-missing", "force"],
        default="force",
        help="'Schema migration mode' panel field.",
    )
    parser.add_argument(
        "--list-app-panel-parameters",
        action="store_true",
        help="Print the capsule's live App Panel schema and exit.",
    )
    parser.add_argument(
        "--list-custom-metadata-fields",
        action="store_true",
        help="Print the deployment's custom metadata schema and exit.",
    )

    # --- Run polling ---
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=30,
        help="Seconds between computation status checks (minimum 5). Default: 30.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Max seconds to wait for the run to finish. Default: no timeout.",
    )

    # --- Publishing the result as a data asset ---
    parser.add_argument("--skip-asset-creation", action="store_true", help="Only run the capsule; don't publish a result asset afterwards.")
    parser.add_argument("--name", help="'Result Name' for the published asset. Required unless --skip-asset-creation.")
    parser.add_argument("--mount", help="'Folder Name' for the published asset. Required unless --skip-asset-creation.")
    parser.add_argument("--description", default="", help="'Description' for the published asset.")
    parser.add_argument("--tags", nargs="*", default=[], help="Tags for the published asset.")
    parser.add_argument("--bucket", help="'Bucket Name': publish to an external S3 bucket instead of Code Ocean's default storage.")
    parser.add_argument("--prefix", help="'Destination Path' within --bucket.")
    parser.add_argument(
        "--wait-for-asset",
        action="store_true",
        help="After publishing, block and poll until the asset reaches 'ready' or 'failed'.",
    )

    args = parser.parse_args()

    listing = args.list_app_panel_parameters or args.list_custom_metadata_fields
    if not listing and not args.skip_asset_creation and (not args.name or not args.mount):
        parser.error("--name and --mount are required unless --skip-asset-creation is used")
    if args.computation_id and args.skip_asset_creation:
        parser.error("--computation-id with --skip-asset-creation would do nothing")

    co_client = get_codeocean_client()

    if args.list_app_panel_parameters:
        list_app_panel_parameters(co_client, args.capsule_id)
        return
    if args.list_custom_metadata_fields:
        list_custom_metadata_fields(co_client)
        return

    computation_id = args.computation_id or run_capsule(co_client, args)

    if args.skip_asset_creation:
        print("Run succeeded. --skip-asset-creation set, not publishing a result asset.")
        return

    asset_params = build_data_asset_params(
        computation_id=computation_id,
        name=args.name,
        mount=args.mount,
        description=args.description,
        tags=args.tags,
        bucket=args.bucket,
        prefix=args.prefix,
        custom_metadata=CUSTOM_METADATA,
    )

    data_asset = co_client.data_assets.create_data_asset(asset_params)
    print(f"Requested data asset creation: id={data_asset.id} state={data_asset.state}")

    if args.wait_for_asset:
        print("Waiting for the asset to finish processing...")
        data_asset = co_client.data_assets.wait_until_ready(data_asset, polling_interval=5)
        print(f"Final asset state: {data_asset.state}")
        if str(data_asset.state) == "failed":
            print(f"Failure reason: {data_asset.failure_reason}")
            sys.exit(1)


if __name__ == "__main__":
    main()
