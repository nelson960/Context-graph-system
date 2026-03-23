from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from context_graph.config import ENTITY_CONFIGS, EntityConfig


METADATA_COLUMNS = ("source_file", "row_hash", "raw_payload")


def discover_entity_files(dataset_root: str | Path, config: EntityConfig) -> list[Path]:
    entity_dir = Path(dataset_root) / config.raw_name
    if not entity_dir.exists():
        raise FileNotFoundError(f"Missing dataset folder: {entity_dir}")
    files = sorted(entity_dir.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No JSONL files found in: {entity_dir}")
    return files


def _hash_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _candidate_join_column(column_name: str, all_primary_key_columns: set[str]) -> bool:
    lowered = column_name.lower()
    return (
        column_name in all_primary_key_columns
        or "reference" in lowered
        or lowered.endswith("document")
        or lowered.endswith("partner")
        or lowered.endswith("customer")
        or lowered.endswith("material")
        or lowered.endswith("addressid")
        or lowered.endswith("plant")
    )


def load_staging_frame(dataset_root: str | Path, config: EntityConfig) -> pd.DataFrame:
    files = discover_entity_files(dataset_root, config)
    records: list[dict[str, Any]] = []
    schema_variants: Counter[tuple[str, ...]] = Counter()
    for file_path in files:
        with file_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                payload = json.loads(line)
                schema_variants[tuple(sorted(payload.keys()))] += 1
                raw_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
                row = dict(payload)
                row["source_file"] = str(file_path.relative_to(Path(dataset_root).parent))
                row["row_hash"] = _hash_payload(raw_payload)
                row["raw_payload"] = raw_payload
                row["_line_number"] = line_number
                records.append(row)
    if not records:
        raise ValueError(f"No records loaded for entity {config.raw_name}")
    frame = pd.DataFrame.from_records(records)
    frame.attrs["file_count"] = len(files)
    frame.attrs["schema_variants"] = schema_variants
    return frame


def load_all_staging_frames(dataset_root: str | Path) -> dict[str, pd.DataFrame]:
    return {
        config.canonical_name: load_staging_frame(dataset_root, config)
        for config in ENTITY_CONFIGS.values()
    }


def profile_staging_frames(staging_frames: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    manifest_rows: list[dict[str, Any]] = []
    column_rows: list[dict[str, Any]] = []
    schema_rows: list[dict[str, Any]] = []
    all_primary_key_columns = {
        column
        for config in ENTITY_CONFIGS.values()
        for column in config.primary_key
    }

    for config in ENTITY_CONFIGS.values():
        frame = staging_frames[config.canonical_name]
        manifest_rows.append(
            {
                "raw_name": config.raw_name,
                "canonical_name": config.canonical_name,
                "file_count": frame.attrs["file_count"],
                "row_count": len(frame),
                "column_count": len(frame.columns),
                "primary_key": ",".join(config.primary_key),
                "schema_variant_count": len(frame.attrs["schema_variants"]),
            }
        )

        for variant_number, (columns, count) in enumerate(
            frame.attrs["schema_variants"].most_common(), start=1
        ):
            schema_rows.append(
                {
                    "canonical_name": config.canonical_name,
                    "variant_number": variant_number,
                    "row_count": count,
                    "columns": json.dumps(columns),
                }
            )

        for column in sorted(frame.columns):
            if column == "_line_number":
                continue
            series = frame[column]
            non_null = series.dropna()
            samples = []
            if not non_null.empty:
                samples = [str(value) for value in non_null.astype(str).unique()[:3]]
            observed_types = sorted({type(value).__name__ for value in non_null.head(25)})
            column_rows.append(
                {
                    "canonical_name": config.canonical_name,
                    "column_name": column,
                    "null_rate": round(float(series.isna().mean()), 6),
                    "non_null_count": int(series.notna().sum()),
                    "candidate_join_column": _candidate_join_column(column, all_primary_key_columns),
                    "observed_types": ",".join(observed_types),
                    "sample_values": json.dumps(samples),
                }
            )

    return (
        pd.DataFrame(manifest_rows).sort_values("canonical_name").reset_index(drop=True),
        pd.DataFrame(column_rows).sort_values(["canonical_name", "column_name"]).reset_index(drop=True),
        pd.DataFrame(schema_rows).sort_values(["canonical_name", "variant_number"]).reset_index(drop=True),
    )
