from datetime import datetime

from backend.src.config import AppConfig
from backend.src.io.utils.parse_delta import parse_delta
from backend.src.config.schemas import TIME_FORMAT, CDAWEB_TIME_FORMAT


def format_time_borders(config: AppConfig) -> list[dict[str, str]]:
    """
    Возвращает список интервалов [{'start': ..., 'end': ...}] для скачивания данных с CDAWeb.
    """

    time_start = datetime.strptime(config.reading.time_start, TIME_FORMAT)
    time_end = datetime.strptime(config.reading.time_end, TIME_FORMAT)

    increase = parse_delta(config.reading.delta)
    current = time_start
    borders: list[dict[str, str]] = []

    while current < time_end:
        end = min(current + increase, time_end)

        borders.append(
            {
                "start": current.strftime(CDAWEB_TIME_FORMAT),
                "end": end.strftime(CDAWEB_TIME_FORMAT)
            }
        )

        current = end

    return borders
