"""Fixtures compartilhadas: AIHs sintéticas no layout do DATASUS e execução do transform em pasta temporária."""

from __future__ import annotations

import itertools
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src import transform

RawRow = dict[str, Any]

# Uma AIH válida de um hospital de Santos. Cada teste sobrescreve só o que importa para ele.
BASE_ROW: RawRow = {
    "N_AIH": None,  # gerado por linha em make_row
    "ANO_CMPT": "2025",
    "MES_CMPT": "01",
    "IDENT": "1",
    "CNES": "2025752",
    "MUNIC_MOV": "354850",
    "MUNIC_RES": "354850",
    "DIAG_PRINC": "J189",
    "DT_INTER": "20250101",
    "DT_SAIDA": "20250105",
    "DIAS_PERM": 4,
    "VAL_TOT": 1000.0,
    "MORTE": 0,
    "COBRANCA": "12",
    "ESPEC": "03",
    "CAR_INT": "02",
    "SEXO": "1",
    "IDADE": 40,
    "COD_IDADE": "4",
}
INT_COLUMNS = ("DIAS_PERM", "MORTE", "IDADE")
SOURCE_FILE = "RDSP9901.parquet"


@pytest.fixture
def make_row() -> Callable[..., RawRow]:
    """Cria uma AIH no layout do Parquet convertido, com N_AIH único quando não for informado."""
    counter = itertools.count(1)

    def _make(**overrides: Any) -> RawRow:
        return {**BASE_ROW, "N_AIH": f"{next(counter):013d}", **overrides}

    return _make


def write_converted(rows: list[RawRow], directory: Path) -> Path:
    """Grava as linhas com os mesmos tipos que o extract produz (texto, Int64 e float64)."""
    directory.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=list(BASE_ROW))
    for col in INT_COLUMNS:
        df[col] = df[col].astype("Int64")
    df["VAL_TOT"] = df["VAL_TOT"].astype("float64")
    path = directory / SOURCE_FILE
    df.to_parquet(path, index=False)
    return path


@pytest.fixture
def trusted_path(tmp_path: Path) -> Path:
    return tmp_path / "trusted.parquet"


@pytest.fixture
def run_transform(tmp_path: Path, trusted_path: Path) -> Callable[[list[RawRow]], pd.DataFrame]:
    """Roda o transform completo (leitura com filtro, regras, validação e gravação) sobre as linhas."""

    def _run(rows: list[RawRow]) -> pd.DataFrame:
        write_converted(rows, tmp_path / "converted")
        return transform.transform(tmp_path / "converted", trusted_path)

    return _run
