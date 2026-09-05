from backend.src.config.schemas import ReadingConfig

CDAWEB_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def format_time_borders(reading: ReadingConfig) -> list[dict[str, str]]:
    """
    Режет интервал чтения на шаги delta для запросов к CDAWeb.

    Returns:
        Список словарей ``{'start': ..., 'end': ...}`` в формате CDAWeb.
    """

    current = reading.time_start
    borders: list[dict[str, str]] = []

    while current < reading.time_end:
        end = min(current + reading.delta, reading.time_end)
        borders.append(
            {
                "start": current.strftime(CDAWEB_TIME_FORMAT),
                "end": end.strftime(CDAWEB_TIME_FORMAT),
            }
        )
        current = end

    return borders
