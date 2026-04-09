import re
from datetime import timedelta


def parse_delta(delta: str) -> timedelta:
    """
    Преобразует строковый шаг (например, '1D', '6H', '1M') в timedelta.

    В ноутбуке 'M' трактовался как 30 дней.
    """
    if not isinstance(delta, str) or not delta.strip():
        raise ValueError("Delta must be a non-empty string like '1D'")

    duration_str, unit = re.findall(r"[A-Za-z]+|\d+", delta.strip())
    duration = int(duration_str)
    unit = unit.upper()

    mapping: dict[str, timedelta] = {
        "M": timedelta(days=duration * 30),
        "D": timedelta(days=duration),
        "H": timedelta(hours=duration),
    }

    if unit not in mapping:
        raise ValueError("Delta unit must be one of: M, D, H")

    return mapping[unit]
