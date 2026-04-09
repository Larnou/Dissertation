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
    """Параметры выборки по времени и спутнику (чтение / скачивание)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    satellite: Satellite = Field(description="Идентификатор спутника THEMIS: A–E.")
    time_start: str = Field(description="Начало интервала в формате 'YYYY-MM-DD HH:MM:SS'.")
    time_end: str = Field(description="Конец интервала в формате 'YYYY-MM-DD HH:MM:SS'.")
    delta: str = Field(description="Шаг скачивания данных в формате значение/единица измерения")

    @field_validator("time_start", "time_end")
    @classmethod
    def validate_datetime_format(cls, value: str) -> str:
        datetime.strptime(value, TIME_FORMAT)
        return value

    @model_validator(mode="after")
    def validate_time_order(self) -> Self:
        start = datetime.strptime(self.time_start, TIME_FORMAT)
        end = datetime.strptime(self.time_end, TIME_FORMAT)
        if end <= start:
            message = "Field time_end must be after time_start"
            raise ValueError(message)
        return self


class WindowFilterConfig(BaseModel):
    """Параметры оконного фильтра (временные окна/константы)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    high_pass: Seconds = Field(ge=1, description="Верхнее окно/граница (сек).")
    low_pass: Seconds = Field(ge=1, description="Нижнее окно/граница (сек).")


class FrequencyFilterConfig(BaseModel):
    """Параметры частотного фильтра (Hz)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bandwidth: Herz = Field(gt=0.0)
    min_frequency: Herz = Field(ge=0.0)
    max_frequency: Herz = Field(gt=0.0)

    @model_validator(mode="after")
    def validate_frequency_range(self) -> Self:
        if self.max_frequency < self.min_frequency:
            message = "Filed max_frequency must be >= min_frequency"
            raise ValueError(message)
        return self


class HParameterConfig(BaseModel):
    """Параметры для расчёта/нормализации (шумы)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    noise_e: Millivolt_per_meter = Field(ge=0.0, description="Оценка шума E-field, мВ/м.")
    noise_vb: Millivolt_per_meter = Field(ge=0.0, description="Оценка погрешности −V×B, мВ/м.")


class PathsConfig(BaseModel):
    """Корневые пути для данных и изображений относительно корня проекта."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    data_root: str = Field(description="Каталог данных относительно корня репозитория.")
    images_root: str = Field(description="Каталог изображений относительно корня репозитория.")


class ResolvedPaths(BaseModel):
    """Абсолютные пути после привязки к корню проекта."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    project_root: Path
    data_root: Path
    images_root: Path


class AppConfig(BaseModel):
    """Корневая модель конфигурации приложения."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    reading: ReadingConfig
    window_filter: WindowFilterConfig
    frequency_filter: FrequencyFilterConfig
    h_parameter: HParameterConfig
    paths: PathsConfig
