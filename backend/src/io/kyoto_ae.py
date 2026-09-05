from datetime import datetime, timedelta
from pathlib import Path
import re

import pandas as pd

KYOTO_AE_LINE_TIME_PATTERN = re.compile(r"^(?P<prefix>\S+)\s+(?P<date>\d{6})E(?P<hour>\d{2})AE\b")
KYOTO_AE_VALUES_PER_HOUR = 60
KYOTO_AE_MISSING_VALUE = 99999


def parse_kyoto_ae_file(path: Path) -> pd.DataFrame:
    """
    Читает один файл Kyoto AE (`.request`) в поминутную таблицу.

    Берёт только строки с маркером `EhhAE`. Часовое среднее в конце строки
    отбрасывается. Код 99999 считается пропуском.

    Args:
        path: путь к файлу в формате KYOTO/E02.
    """

    rows: list[dict[str, object]] = []
    path = Path(path)

    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue

        match = KYOTO_AE_LINE_TIME_PATTERN.search(line)
        if match is None:
            continue

        hour_start = datetime.strptime(f"{match.group('date')}{match.group('hour')}", "%y%m%d%H")
        values = _extract_minute_values(line, path, line_number)

        rows.extend(
            {"Time": hour_start + timedelta(minutes=minute), "AE": ae_value}
            for minute, ae_value in enumerate(values[:KYOTO_AE_VALUES_PER_HOUR])
        )

    return _normalize_kyoto_ae_dataframe(pd.DataFrame(rows, columns=["Time", "AE"]))


def read_kyoto_ae_directory(input_dir: Path) -> pd.DataFrame:
    """
    Reads all Kyoto AE `*.request` files from a directory into one DataFrame.
    """

    input_dir = Path(input_dir)
    paths = sorted(input_dir.glob("*.request"))
    if not paths:
        raise FileNotFoundError(f"No Kyoto AE .request files found in {input_dir}")

    dataframes = [parse_kyoto_ae_file(path) for path in paths]
    return _normalize_kyoto_ae_dataframe(pd.concat(dataframes, ignore_index=True))


def save_kyoto_ae_to_parquet(dataframe: pd.DataFrame, output_path: Path) -> None:
    """
    Saves a Kyoto AE DataFrame as parquet.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _normalize_kyoto_ae_dataframe(dataframe).to_parquet(output_path, index=False)


def _extract_minute_values(line: str, path: Path, line_number: int) -> list[int]:
    numeric_tokens = [int(token) for token in line.split() if re.fullmatch(r"[-+]?\d+", token)]
    if len(numeric_tokens) < KYOTO_AE_VALUES_PER_HOUR:
        raise ValueError(
            f"Expected at least {KYOTO_AE_VALUES_PER_HOUR} AE values in {path} at line {line_number}, "
            f"got {len(numeric_tokens)}"
        )
    return numeric_tokens


def _normalize_kyoto_ae_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame(columns=["Time", "AE"])

    result = dataframe.loc[:, ["Time", "AE"]].copy()
    result["Time"] = pd.to_datetime(result["Time"])
    result["AE"] = pd.to_numeric(result["AE"], errors="coerce")
    result.loc[result["AE"] == KYOTO_AE_MISSING_VALUE, "AE"] = pd.NA
    result = result.dropna(subset=["Time", "AE"])
    result = result.drop_duplicates(subset=["Time"])
    result = result.sort_values("Time").reset_index(drop=True)
    return result
