"""Leitor de arquivos DBF (dBase III) vetorizado com numpy.

Usado no lugar do dbfread por dois motivos verificados com arquivos reais do DATASUS:

- Os arquivos ST do CNES vêm sem o byte 0x0D que encerra o cabeçalho, e o dbfread
  quebra ao lê-los. Aqui o número de campos sai do tamanho de cabeçalho declarado.
- É ~4x mais rápido lendo todas as colunas de um RDSP (8s contra 33s) e ~35x lendo
  só as colunas necessárias.

Os tipos seguem o mesmo critério do dbfread: campos N/F viram números (inteiro
anulável quando não há casas decimais), D vira data e o resto vira texto sem os
espaços de preenchimento.
"""

from __future__ import annotations

import logging
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_HEADER_PREFIX_SIZE = 32
_FIELD_DESCRIPTOR_SIZE = 32
_HEADER_TERMINATOR = 0x0D
_DELETED_FLAG = ord("*")
_MAX_INT64_DIGITS = 18


class DBFFormatError(ValueError):
    """Arquivo DBF truncado ou com cabeçalho inconsistente."""


@dataclass(frozen=True)
class DBFField:
    name: str
    type: str
    offset: int  # posição dentro do registro; o byte 0 é a marca de exclusão
    length: int
    decimals: int


def _parse_fields(raw: np.ndarray, header_len: int, record_len: int) -> list[DBFField]:
    max_fields = (header_len - _HEADER_PREFIX_SIZE - 1) // _FIELD_DESCRIPTOR_SIZE
    fields: list[DBFField] = []
    offset = 1
    for i in range(max_fields):
        start = _HEADER_PREFIX_SIZE + i * _FIELD_DESCRIPTOR_SIZE
        if raw[start] == _HEADER_TERMINATOR:
            break
        desc = raw[start : start + _FIELD_DESCRIPTOR_SIZE].tobytes()
        name = desc[:11].split(b"\x00", 1)[0].decode("ascii").strip()
        fields.append(DBFField(name, chr(desc[11]), offset, desc[16], desc[17]))
        offset += desc[16]

    if offset != record_len:
        raise DBFFormatError(
            f"Soma dos tamanhos dos campos ({offset}) difere do tamanho do registro ({record_len})"
        )
    return fields


def _decode_field(records: np.ndarray, field: DBFField, encoding: str) -> pd.Series:
    chunk = np.ascontiguousarray(records[:, field.offset : field.offset + field.length])
    text = pd.Series(chunk.view(f"S{field.length}").ravel()).str.decode(encoding).str.strip()

    if field.type in ("N", "F"):
        numbers = pd.to_numeric(text.mask(text == ""), errors="coerce")
        invalid = int((numbers.isna() & (text != "")).sum())
        if invalid:
            logger.warning(
                "Campo %s: %d valor(es) não numérico(s) convertido(s) para nulo", field.name, invalid
            )
        if field.decimals == 0 and field.length <= _MAX_INT64_DIGITS:
            return numbers.astype("Int64")
        return numbers.astype("float64")

    if field.type == "D":
        return pd.to_datetime(text, format="%Y%m%d", errors="coerce")

    return text


def read_dbf(
    path: Path,
    columns: Iterable[str] | None = None,
    encoding: str = "latin-1",
) -> pd.DataFrame:
    """Lê um arquivo .dbf para DataFrame, opcionalmente só com as colunas pedidas.

    Registros marcados como excluídos são descartados.

    Raises:
        DBFFormatError: arquivo menor que o declarado no cabeçalho ou campos inconsistentes.
        KeyError: alguma coluna pedida não existe no arquivo.
    """
    raw = np.fromfile(path, dtype=np.uint8)
    if raw.size < _HEADER_PREFIX_SIZE:
        raise DBFFormatError(f"{path.name}: arquivo com {raw.size} bytes, pequeno demais para um DBF")

    n_records, header_len, record_len = struct.unpack("<IHH", raw[4:12].tobytes())
    fields = _parse_fields(raw, header_len, record_len)

    data_end = header_len + n_records * record_len
    if raw.size < data_end:
        raise DBFFormatError(
            f"{path.name}: truncado ({raw.size:,} bytes; o cabeçalho declara {data_end:,})"
        )

    if columns is not None:
        wanted = set(columns)
        missing = wanted - {f.name for f in fields}
        if missing:
            raise KeyError(f"{path.name}: colunas inexistentes: {sorted(missing)}")
        fields = [f for f in fields if f.name in wanted]

    records = raw[header_len:data_end].reshape(n_records, record_len)
    deleted = records[:, 0] == _DELETED_FLAG
    if deleted.any():
        logger.info("%s: %d registro(s) marcado(s) como excluído(s) descartado(s)", path.name, deleted.sum())
        records = records[~deleted]

    return pd.DataFrame({f.name: _decode_field(records, f, encoding) for f in fields})
