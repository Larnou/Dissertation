from datetime import datetime
from pathlib import Path
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
        Проверка валидации введённой даты.

        Args:
            value: Строка во вводимом формате.

        Returns:
            Изначальную строку с датой
        """

        datetime.strptime(value, TIME_FORMAT)
        return value

    @model_validator(mode="after")
    def validate_time_order(self) -> Self:
        """
        Валидация того, что была правильно введена дата.

        Время начала скачивания данных должно идти раньше времени оканчания скачивания данных.

        Returns:
            Если проверка прошла успешно, возвращает отвадированный объект.
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
        Валидация правильного размера окна для оконных фильтров. Низкочастотный должен иметь больший период.

        Returns:
            В случае успешной валидации возвращает валидированный объект.
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
        Валидация полосового частотного фильтра.

        Границы фильтра должны быть между min_frequency и max_frequency.

        Returns:
            В случае успешной валидации возвращает валидированный объект.
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
    """Корневые пути для артефактов относительно корня проекта."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    data: str = Field(description="Каталог parquet-данных относительно корня репозитория.")
    periods: str = Field(description="Каталог CSV-периодов относительно корня репозитория.")
    matrices: str = Field(description="Каталог матриц/распределений относительно корня репозитория.")
    images: str = Field(description="Каталог изображений относительно корня репозитория.")
    distributions: str = Field(description="Каталог распределений параметров.")


class ResolvedPaths(BaseModel):
    """
    Абсолютные пути после привязки к корню проекта.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    data: Path
    periods: Path
    matrices: Path
    images: Path
    distributions: Path


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
