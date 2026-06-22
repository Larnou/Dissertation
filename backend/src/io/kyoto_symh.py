from datetime import datetime, timedelta
from pathlib import Path
import re

import pandas as pd

KYOTO_SYMH_LINE_TIME_PATTERN = re.compile(r"^\S+\s+\S*?(?P<date>\d{6})D(?P<hour>\d{2})ASY")
KYOTO_SYMH_VALUES_PER_HOUR = 60


def parse_kyoto_symh_file(path: Path) -> pd.DataFrame:
    """
    Reads one Kyoto SYM-H `.txt` file into a minute-resolution DataFrame.

    Each data line contains a YYMMDD/hour marker and 60 minute values. Kyoto
    appends an additional summary value at the end of the line, which is ignored.
    """

    rows: list[dict[str, object]] = []
    path = Path(path)

    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue

        match = KYOTO_SYMH_LINE_TIME_PATTERN.search(line)
        if match is None:
            continue

        hour_start = datetime.strptime(f"{match.group('date')}{match.group('hour')}", "%y%m%d%H")
        values = _extract_minute_values(line, path, line_number)

        rows.extend(
            {"Time": hour_start + timedelta(minutes=minute), "SYMH": symh_value}
            for minute, symh_value in enumerate(values[:KYOTO_SYMH_VALUES_PER_HOUR])
        )

    return _normalize_kyoto_symh_dataframe(pd.DataFrame(rows, columns=["Time", "SYMH"]))


def read_kyoto_symh_directory(input_dir: Path) -> pd.DataFrame:
    """
    Reads all Kyoto SYM-H `*.txt` files from a directory into one DataFrame.
    """

    input_dir = Path(input_dir)
    paths = sorted(input_dir.glob("*.txt"))
    if not paths:
        raise FileNotFoundError(f"No Kyoto SYM-H .txt files found in {input_dir}")

    dataframes = [parse_kyoto_symh_file(path) for path in paths]
    return _normalize_kyoto_symh_dataframe(pd.concat(dataframes, ignore_index=True))


def save_kyoto_symh_to_parquet(dataframe: pd.DataFrame, output_path: Path) -> None:
    """
    Saves a Kyoto SYM-H DataFrame as parquet.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _normalize_kyoto_symh_dataframe(dataframe).to_parquet(output_path, index=False)


def _extract_minute_values(line: str, path: Path, line_number: int) -> list[int]:
    numeric_tokens = [int(token) for token in line.split() if re.fullmatch(r"[-+]?\d+", token)]
    if len(numeric_tokens) < KYOTO_SYMH_VALUES_PER_HOUR:
        raise ValueError(
            f"Expected at least {KYOTO_SYMH_VALUES_PER_HOUR} SYM-H values in {path} at line {line_number}, "
            f"got {len(numeric_tokens)}"
        )
    return numeric_tokens


def _normalize_kyoto_symh_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame(columns=["Time", "SYMH"])

    result = dataframe.loc[:, ["Time", "SYMH"]].copy()
    result["Time"] = pd.to_datetime(result["Time"])
    result["SYMH"] = pd.to_numeric(result["SYMH"], errors="coerce")
    result = result.dropna(subset=["Time", "SYMH"])
    result = result.drop_duplicates(subset=["Time"])
    result = result.sort_values("Time").reset_index(drop=True)
    return result
