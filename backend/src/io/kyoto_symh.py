from datetime import datetime, timedelta
from pathlib import Path
import re

import pandas as pd

# WDC Kyoto: колонка 19 = H/D, колонки 22-24 = SYM/ASY. SYM-H — это HhhSYM, не DhhASY (ASY-D).
KYOTO_SYMH_LINE_TIME_PATTERN = re.compile(r"^\S+\s+\S*?(?P<date>\d{6})H(?P<hour>\d{2})SYM")
KYOTO_SYMH_VALUES_PER_HOUR = 60
KYOTO_SYMH_MISSING_VALUE = 99999


def parse_kyoto_symh_file(path: Path) -> pd.DataFrame:
    """
    Читает один файл Kyoto SYM-H (`.txt`) в поминутную таблицу.

    Берёт только строки с маркером `HhhSYM`. В каждой такой строке 60 минутных
    значений в нТл и часовое среднее в конце; среднее отбрасывается.
    Код 99999 считается пропуском.

    Args:
        path: путь к файлу в формате WDC ASY/SYM.
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
    Собирает все `*.txt` из каталога в одну поминутную таблицу SYM-H.

    Args:
        input_dir: каталог с исходниками WDC ASY/SYM.
    """

    input_dir = Path(input_dir)
    paths = sorted(input_dir.glob("*.txt"))
    if not paths:
        raise FileNotFoundError(f"No Kyoto SYM-H .txt files found in {input_dir}")

    dataframes = [parse_kyoto_symh_file(path) for path in paths]
    return _normalize_kyoto_symh_dataframe(pd.concat(dataframes, ignore_index=True))


def save_kyoto_symh_to_parquet(dataframe: pd.DataFrame, output_path: Path) -> None:
    """
    Сохраняет таблицу SYM-H в parquet.

    Args:
        dataframe: колонки Time и SYMH.
        output_path: путь к `symh.parquet`.
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
    result.loc[result["SYMH"] == KYOTO_SYMH_MISSING_VALUE, "SYMH"] = pd.NA
    result = result.dropna(subset=["Time", "SYMH"])
    result = result.drop_duplicates(subset=["Time"])
    result = result.sort_values("Time").reset_index(drop=True)
    return result
