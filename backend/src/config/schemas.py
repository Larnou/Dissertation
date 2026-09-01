from datetime import datetime
from typing import Self, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
CDAWEB_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# --- алиасы типов / единиц измерения (для читаемости кода) ---
Satellite = Literal["A", "B", "C", "D", "E"]
Herz = float
Seconds = int
Millivolt_per_meter = float


class ReadingConfig(BaseModel):
    """
    Параметры выборки по времени и спутнику (чтение / скачивание).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    satellite: Satellite = Field(description="Идентификатор спутника THEMIS: A–E.")
    time_start: str = Field(description="Начало интервала в формате 'YYYY-MM-DD HH:MM:SS'.")
    time_end: str = Field(description="Конец интервала в формате 'YYYY-MM-DD HH:MM:SS'.")
    delta: str = Field(description="Шаг скачивания данных в формате значение/единица измерения")

    @field_validator("time_start", "time_end")
    @classmethod
    def validate_datetime_format(cls, value: str) -> str:
        """
        Проверяет формат даты и времени.

        Args:
            value: Строка в формате ``YYYY-MM-DD HH:MM:SS``.

        Returns:
            Исходная строка при успешной валидации.
        """

        datetime.strptime(value, TIME_FORMAT)
        return value

    @model_validator(mode="after")
    def validate_time_order(self) -> Self:
        """
        Проверяет корректный порядок временных границ.
        Время окончания должно быть строго позже времени начала.

        Returns:
            Валидированный экземпляр конфигурации.
        """
        start = datetime.strptime(self.time_start, TIME_FORMAT)
        end = datetime.strptime(self.time_end, TIME_FORMAT)

        if end <= start:
            message = f"Field time_end={end} must be after time_start={start}"
            raise ValueError(message)
        return self


class WindowFilterConfig(BaseModel):
    """
    Длительности периода колебаний (секунды), на которых строятся окна свёртки.

    Low_pass отвечает за низкочастотную фильтрацию, high_pass за высокочастотную фильтрацию.
    Ожидается low_pass > high_pass (например 600 и 45).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    low_pass: Seconds = Field(
        ge=1,
        description="Длительность периода для низкочастотной (широкой) ветви фильтрации, сек.",
    )
    high_pass: Seconds = Field(
        ge=1,
        description="Длительность периода для высокочастотной (узкой) ветви и окна скользящего среднего в FAH, сек.",
    )

    @model_validator(mode="after")
    def validate_period_order(self) -> Self:
        """
        Проверяет согласованность периодов оконных фильтров.
        Низкочастотная ветвь должна иметь больший период, чем высокочастотная.

        Returns:
            Валидированный экземпляр конфигурации.
        """

        if self.low_pass < self.high_pass:
            message = f"Low_pass={self.low_pass} должен быть больше high_pass={self.high_pass} (длинный период > короткий)."
            raise ValueError(message)
        return self


class FrequencyFilterConfig(BaseModel):
    """
    Параметры частотного полосового фильтра (Hz).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    bandwidth: Herz = Field(gt=0.0)
    min_frequency: Herz = Field(gt=0.0)
    max_frequency: Herz = Field(gt=0.0)

    @model_validator(mode="after")
    def validate_frequency_range(self) -> Self:
        """
        Проверяет корректность диапазона полосового фильтра.
        Верхняя граница должна быть не меньше нижней.

        Returns:
            Валидированный экземпляр конфигурации.
        """

        if self.max_frequency < self.min_frequency:
            message = f"Filed max_frequency={self.max_frequency} must be >= min_frequency={self.min_frequency}"
            raise ValueError(message)
        return self


class HParameterConfig(BaseModel):
    """
    Параметры для расчёта/нормализации определения параметра Н с учётом фонового шума.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    noise_e: Millivolt_per_meter = Field(ge=0.0, description="Оценка шума E-field, мВ/м.")
    noise_vb: Millivolt_per_meter = Field(ge=0.0, description="Оценка погрешности −V×B, мВ/м.")


class PathsConfig(BaseModel):
    """
    Корневые каталоги данных относительно корня репозитория.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    data_root: str = Field(description="Корень backend/data/.")
    events: str = Field(description="Каталог событий THEMIS (backend/data/events).")


class AppConfig(BaseModel):
    """
    Корневая модель конфигурации приложения.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    reading: ReadingConfig
    window_filter: WindowFilterConfig
    frequency_filter: FrequencyFilterConfig
    h_parameter: HParameterConfig
    paths: PathsConfig
