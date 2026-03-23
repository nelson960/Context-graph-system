from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

import pandas as pd

from context_graph.config import ENTITY_CONFIGS, EntityConfig


ZERO_PADDED_COLUMN_NAMES = {
    "salesOrderItem",
    "referenceSdDocumentItem",
    "deliveryDocumentItem",
    "billingDocumentItem",
    "accountingDocumentItem",
    "scheduleLine",
}
CANONICAL_METADATA_COLUMNS = ("source_file", "row_hash", "raw_payload")


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _should_strip_leading_zeros(column_name: str) -> bool:
    return column_name in ZERO_PADDED_COLUMN_NAMES


def _normalize_identifier(value: Any, column_name: str) -> str | None:
    if _is_blank(value):
        return None
    if isinstance(value, (dict, list)):
        raise ValueError(f"Identifier column {column_name} received nested value: {value!r}")
    normalized = str(value).strip()
    if normalized == "":
        return None
    if _should_strip_leading_zeros(column_name):
        stripped = normalized.lstrip("0")
        return stripped if stripped != "" else "0"
    return normalized


def _normalize_bool(value: Any, column_name: str) -> bool | None:
    if _is_blank(value):
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "t", "1", "x", "y", "yes"}:
        return True
    if normalized in {"false", "f", "0", "n", "no"}:
        return False
    raise ValueError(f"Invalid boolean value for {column_name}: {value!r}")


def _normalize_time_value(value: Any, column_name: str) -> str | None:
    if _is_blank(value):
        return None
    if isinstance(value, dict):
        try:
            hours = int(value["hours"])
            minutes = int(value["minutes"])
            seconds = int(value["seconds"])
        except KeyError as exc:
            raise ValueError(f"Invalid time payload for {column_name}: {value!r}") from exc
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    normalized = str(value).strip()
    if normalized == "":
        return None
    return normalized


def _normalize_datetime_value(value: Any, column_name: str) -> str | None:
    if _is_blank(value):
        return None
    if isinstance(value, pd.Timestamp):
        timestamp = value.to_pydatetime()
    elif isinstance(value, datetime):
        timestamp = value
    else:
        normalized = str(value).strip()
        if normalized == "":
            return None
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        try:
            timestamp = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Column {column_name} contains non-datetime value: {value!r}") from exc
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_scalar(value: Any) -> Any:
    if _is_blank(value):
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    if isinstance(value, str):
        return value.strip()
    return value


def _normalize_numeric_series(series: pd.Series, column_name: str) -> pd.Series:
    cleaned = series.map(lambda value: None if _is_blank(value) else value)
    parsed = pd.to_numeric(cleaned, errors="coerce")
    invalid_mask = cleaned.notna() & parsed.isna()
    if invalid_mask.any():
        bad_values = cleaned[invalid_mask].astype(str).tolist()[:5]
        raise ValueError(f"Column {column_name} contains non-numeric values: {bad_values}")
    return parsed


def _normalize_datetime_series(series: pd.Series, column_name: str) -> pd.Series:
    return series.map(lambda value: _normalize_datetime_value(value, column_name))


def normalize_frame(frame: pd.DataFrame, config: EntityConfig) -> pd.DataFrame:
    missing_columns = [column for column in config.primary_key if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns for {config.canonical_name}: {missing_columns}")

    normalized = pd.DataFrame(index=frame.index)
    for column in frame.columns:
        if column == "_line_number":
            continue
        series = frame[column]
        if column in config.id_columns or column in config.primary_key:
            normalized[column] = series.map(lambda value: _normalize_identifier(value, column))
        elif column in config.numeric_columns:
            normalized[column] = _normalize_numeric_series(series, column)
        elif column in config.datetime_columns:
            normalized[column] = _normalize_datetime_series(series, column)
        elif column in config.time_columns:
            normalized[column] = series.map(lambda value: _normalize_time_value(value, column))
        elif column in config.bool_columns:
            normalized[column] = series.map(lambda value: _normalize_bool(value, column))
        else:
            normalized[column] = series.map(_normalize_scalar)

    null_primary_keys = normalized[list(config.primary_key)].isna().any(axis=1)
    if null_primary_keys.any():
        bad_rows = normalized.loc[null_primary_keys, list(config.primary_key) + ["source_file"]].head(5)
        raise ValueError(
            f"Null primary key values found in {config.canonical_name}: {bad_rows.to_dict(orient='records')}"
        )
    return normalized


def deduplicate_frame(frame: pd.DataFrame, config: EntityConfig) -> tuple[pd.DataFrame, dict[str, Any]]:
    duplicate_mask = frame.duplicated(subset=list(config.primary_key), keep=False)
    if not duplicate_mask.any():
        return frame.reset_index(drop=True), {
            "canonical_name": config.canonical_name,
            "input_rows": len(frame),
            "output_rows": len(frame),
            "dropped_rows": 0,
            "duplicate_key_count": 0,
        }

    duplicate_groups = frame.loc[duplicate_mask].groupby(list(config.primary_key), dropna=False, sort=False)
    kept_indices: list[int] = []
    resolved_groups = 0

    for _, group in duplicate_groups:
        payload_columns = [column for column in group.columns if column not in CANONICAL_METADATA_COLUMNS]
        if len(group[payload_columns].drop_duplicates()) == 1:
            kept_indices.append(group.index[0])
            resolved_groups += 1
            continue

        precedence_columns = [column for column in config.precedence_columns if column in group.columns]
        if not precedence_columns:
            raise ValueError(
                f"Conflicting duplicates found in {config.canonical_name} without precedence columns for key "
                f"{group[list(config.primary_key)].iloc[0].to_dict()}"
            )

        sorted_group = group.sort_values(precedence_columns + ["source_file"], ascending=False, na_position="last")
        top_row = sorted_group.iloc[0]
        top_precedence = tuple(top_row[column] for column in precedence_columns)
        tied = sorted_group[
            sorted_group.apply(
                lambda row: tuple(row[column] for column in precedence_columns) == top_precedence,
                axis=1,
            )
        ]
        if len(tied[payload_columns].drop_duplicates()) > 1:
            raise ValueError(
                f"Conflicting duplicates in {config.canonical_name} remain tied after precedence sort for key "
                f"{group[list(config.primary_key)].iloc[0].to_dict()}"
            )
        kept_indices.append(sorted_group.index[0])
        resolved_groups += 1

    unique_rows = frame.loc[~duplicate_mask]
    deduplicated = pd.concat([unique_rows, frame.loc[kept_indices]], ignore_index=False).sort_index()
    return deduplicated.reset_index(drop=True), {
        "canonical_name": config.canonical_name,
        "input_rows": len(frame),
        "output_rows": len(deduplicated),
        "dropped_rows": len(frame) - len(deduplicated),
        "duplicate_key_count": int(duplicate_mask.sum()),
        "resolved_duplicate_groups": resolved_groups,
    }


def normalize_all_frames(staging_frames: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    canonical_frames: dict[str, pd.DataFrame] = {}
    reports: list[dict[str, Any]] = []
    for config in ENTITY_CONFIGS.values():
        normalized = normalize_frame(staging_frames[config.canonical_name], config)
        deduplicated, report = deduplicate_frame(normalized, config)
        canonical_frames[config.canonical_name] = deduplicated
        reports.append(report)
    return canonical_frames, pd.DataFrame(reports).sort_values("canonical_name").reset_index(drop=True)
