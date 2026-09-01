from datetime import datetime, timedelta
import re
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
Satellite = Literal["A", "B", "C", "D", "E"]

_DURATION_PATTERN = re.compile(r"^(\d+)([MDH])$", re.IGNORECASE)


def parse_duration(value: str | timedelta) -> timedelta:
    """
    Преобразует шаг из конфига в timedelta.

    В JSON шаг задаётся строкой: число и единица ``M`` / ``D`` / ``H``.
    ``M`` — 30 дней, как в исходных ноутбуках.

    Args:
        value: уже готовый интервал или строка вида ``1M``, ``6H``, ``1D``.

    Returns:
        Положительный timedelta.

    Raises:
        ValueError: пустая строка, неизвестная единица или неположительная длительность.
        TypeError: значение не строка и не timedelta.
    """

    if isinstance(value, timedelta):
        if value <= timedelta(0):
            raise ValueError("delta должен быть положительным")
        return value

    if not isinstance(value, str) or not value.strip():
        raise ValueError("delta должен быть непустой строкой вида '1D'")

    match = _DURATION_PATTERN.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Некорректный delta={value!r}, ожидается формат '1M', '1D' или '6H'")

    amount = int(match.group(1))
    unit = match.group(2).upper()
    if amount <= 0:
        raise ValueError("delta должен быть положительным")

    if unit == "M":
        return timedelta(days=amount * 30)
    if unit == "D":
        return timedelta(days=amount)
    return timedelta(hours=amount)


def _parse_clock(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, TIME_FORMAT)
        except ValueError as exc:
            raise ValueError(f"Ожидается формат '{TIME_FORMAT}', получено {value!r}") from exc
    raise TypeError(f"Некорректный тип даты: {type(value)!r}")


class ReadingConfig(BaseModel):
    """
    Параметры выборки по времени и спутнику (чтение / скачивание).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    satellite: Satellite = Field(description="Идентификатор спутника THEMIS: A–E.")
    time_start: datetime = Field(description="Начало интервала, naive UTC.")
    time_end: datetime = Field(description="Конец интервала, naive UTC.")
    delta: timedelta = Field(description="Шаг скачивания с CDAWeb.")

    @field_validator("time_start", "time_end", mode="before")
    @classmethod
    def validate_datetime_format(cls, value: object) -> datetime:
        """
        Разбирает строку ``YYYY-MM-DD HH:MM:SS`` в datetime.
        """

        return _parse_clock(value)

    @field_validator("delta", mode="before")
    @classmethod
    def validate_delta(cls, value: object) -> timedelta:
        """
        Разбирает шаг скачивания из строки конфига.
        """

        if isinstance(value, str | timedelta):
            return parse_duration(value)
        raise TypeError(f"Некорректный тип delta: {type(value)!r}")

    @model_validator(mode="after")
    def validate_time_order(self) -> Self:
        """
        Проверяет, что конец интервала строго позже начала.
        """

        if self.time_end <= self.time_start:
            message = f"time_end={self.time_end} должен быть позже time_start={self.time_start}"
            raise ValueError(message)
        return self


class WindowFilterConfig(BaseModel):
    """
    Длительности периода колебаний (секунды), на которых строятся окна свёртки.

    low_pass — низкочастотная ветвь, high_pass — высокочастотная.
    Ожидается low_pass >= high_pass (например 600 и 45).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    low_pass: int = Field(
        ge=1,
        description="Длительность периода для низкочастотной (широкой) ветви фильтрации, сек.",
    )
    high_pass: int = Field(
        ge=1,
        description="Длительность периода для высокочастотной (узкой) ветви и окна скользящего среднего в FAH, сек.",
    )

    @model_validator(mode="after")
    def validate_period_order(self) -> Self:
        """
        Проверяет, что низкочастотная ветвь не короче высокочастотной.
        """

        if self.low_pass < self.high_pass:
            message = f"low_pass={self.low_pass} должен быть >= high_pass={self.high_pass} (длинный период >= короткий)."
            raise ValueError(message)
        return self


class HParameterConfig(BaseModel):
    """
    Параметры для расчёта H с учётом фонового шума.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    noise_e: float = Field(ge=0.0, description="Оценка шума E-field, мВ/м.")
    noise_vb: float = Field(ge=0.0, description="Оценка погрешности −V×B, мВ/м.")


class PathsConfig(BaseModel):
    """
    Корень данных относительно корня репозитория.

    Каталог событий строится как ``{data_root}/events``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    data_root: str = Field(description="Корень данных относительно корня репозитория, обычно backend/data.")


class AppConfig(BaseModel):
    """
    Корневая модель конфигурации приложения.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    reading: ReadingConfig
    window_filter: WindowFilterConfig
    h_parameter: HParameterConfig
    paths: PathsConfig
