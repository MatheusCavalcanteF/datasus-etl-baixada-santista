"""Leitor de arquivos DBF (dBase III) vetorizado com numpy.

Usado no lugar do dbfread por dois motivos verificados com arquivos reais do DATASUS:

- Os arquivos ST do CNES vêm sem o byte 0x0D que encerra o cabeçalho, e o dbfread
  quebra ao lê-los. Aqui o número de campos sai do tamanho de cabeçalho declarado.
- É ~3x mais rápido lendo todas as colunas de um RDSP (10-12s contra 31-33s), com
  ~5x menos memória. A medição está em benchmarks/.

Os tipos seguem o mesmo critério do dbfread: campos N/F viram números (inteiro
anulável quando não há casas decimais), D vira data, L vira booleano anulável e C
vira texto sem os espaços de preenchimento. Diferente do dbfread, valor inválido
vira nulo com aviso no log em vez de exceção, e tipo de campo sem suporte (memo,
binários do FoxPro) levanta DBFFormatError em vez de virar texto sem sentido.

O encoding do texto sai do byte de idioma do cabeçalho (LDID). Os RD do SIH
declaram cp850: em latin-1, o "ç" (0x87) de AUD_JUST virava um caractere de controle.
Os ST do CNES não declaram (LDID 0) e usam latin-1: o "º" de ALVARA é 0xBA.
A decodificação nunca perde bytes: o AD do SIA declara cp1252 mas guarda em
AP_CNSPCN um identificador codificado com 0x81, byte que o cp1252 não define.
"""

from __future__ import annotations

import codecs
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
_EOF_MARKER = 0x1A  # opcional no dBase; os RD do DATASUS terminam sem ele
_ACTIVE_FLAG = ord(" ")
_DELETED_FLAG = ord("*")
_MAX_INT64_DIGITS = 18
_LDID_OFFSET = 29  # byte do cabeçalho com o "language driver" (code page do texto)

# LDIDs do dBase/FoxPro que aparecem em arquivos brasileiros
LDID_ENCODINGS = {
    0x01: "cp437",
    0x02: "cp850",
    0x03: "cp1252",
    0x57: "cp1252",
    0x58: "cp1252",  # ST antigos do CNES (ex.: STAC0508)
    0x64: "cp852",
}
DEFAULT_ENCODING = "latin-1"  # quando o cabeçalho não declara code page (LDID 0) ou declara uma desconhecida

_LOSSLESS_ERRORS = "dbf_reader.lossless"


def _byte_as_code_point(error: UnicodeError) -> tuple[str, int]:
    # Byte sem caractere na code page (0x81 no cp1252) vira o code point de mesmo
    # número (U+0081), como faz o decodificador windows-1252 do WHATWG
    if not isinstance(error, UnicodeDecodeError):
        raise error
    return error.object[error.start : error.end].decode("latin-1"), error.end


codecs.register_error(_LOSSLESS_ERRORS, _byte_as_code_point)

SUPPORTED_TYPES = frozenset("CNFDL")
_TRUE_VALUES = frozenset("TtYy")
_FALSE_VALUES = frozenset("FfNn")


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
        try:
            name = desc[:11].split(b"\x00", 1)[0].decode("ascii").strip()
        except UnicodeDecodeError:
            raise DBFFormatError(f"Nome do campo {i + 1} não é ASCII: {desc[:11]!r}") from None
        fields.append(DBFField(name, chr(desc[11]), offset, desc[16], desc[17]))
        offset += desc[16]

    if offset != record_len:
        raise DBFFormatError(
            f"Soma dos tamanhos dos campos ({offset}) difere do tamanho do registro ({record_len})"
        )
    return fields


def _header_encoding(raw: np.ndarray, name: str) -> str:
    ldid = int(raw[_LDID_OFFSET])
    encoding = LDID_ENCODINGS.get(ldid)
    if encoding is None:
        # LDID 0 é comum (ST do CNES) e não declara nada; um valor desconhecido merece mais atenção
        logger.log(
            logging.INFO if ldid == 0 else logging.WARNING,
            "%s: cabeçalho sem code page conhecida (LDID 0x%02X); texto lido como %s",
            name,
            ldid,
            DEFAULT_ENCODING,
        )
        return DEFAULT_ENCODING
    return encoding


def _decode_field(records: np.ndarray, field: DBFField, encoding: str) -> pd.Series:
    chunk = np.ascontiguousarray(records[:, field.offset : field.offset + field.length])
    raw_values = pd.Series(chunk.view(f"S{field.length}").ravel())
    text = raw_values.str.decode(encoding, errors=_LOSSLESS_ERRORS).str.strip()

    if field.type in ("N", "F"):
        # numpy_nullable lê inteiros direto como Int64; passar por float64 perderia
        # precisão acima de 15 dígitos sempre que a coluna tivesse algum vazio
        numbers = pd.to_numeric(text.mask(text == ""), errors="coerce", dtype_backend="numpy_nullable")
        _warn_invalid(field, numbers.isna() & (text != ""), "não numérico(s)")
        if field.decimals == 0 and field.length <= _MAX_INT64_DIGITS:
            # Checagem explícita: o astype("Int64") do pandas 3 trunca 1.5 para 1 sem erro
            fractional = (numbers % 1 != 0).fillna(False)
            if not fractional.any():
                return numbers.astype("Int64")
            logger.warning(
                "Campo %s: declarado sem casas decimais, mas tem %d valor(es) fracionário(s); lido como float",
                field.name,
                int(fractional.sum()),
            )
        return numbers.astype("float64")

    if field.type == "D":
        blank = text.str.strip("0") == ""  # o dbfread também trata 00000000 como data vazia
        dates = pd.to_datetime(text.mask(blank), format="%Y%m%d", errors="coerce")
        _warn_invalid(field, dates.isna() & ~blank, "de data inválida(s)")
        return dates

    if field.type == "L":
        logical = pd.Series(pd.NA, index=text.index, dtype="boolean")
        logical[text.isin(_TRUE_VALUES)] = True
        logical[text.isin(_FALSE_VALUES)] = False
        _warn_invalid(field, logical.isna() & ~text.isin({"", "?"}), "lógico(s) inválido(s)")
        return logical

    return text


def _warn_invalid(field: DBFField, invalid: pd.Series, description: str) -> None:
    count = int(invalid.sum())
    if count:
        logger.warning("Campo %s: %d valor(es) %s convertido(s) para nulo", field.name, count, description)


def read_dbf(
    path: Path,
    columns: Iterable[str] | None = None,
    encoding: str | None = None,
) -> pd.DataFrame:
    """Lê um arquivo .dbf para DataFrame. Detalhes em read_dbf_bytes."""
    return _read(np.fromfile(path, dtype=np.uint8), path.name, columns, encoding)


def read_dbf_bytes(
    data: bytes,
    name: str,
    columns: Iterable[str] | None = None,
    encoding: str | None = None,
) -> pd.DataFrame:
    """Lê um .dbf já em memória (ex.: saída do datasus_dbc.decompress_bytes) para DataFrame.

    `name` só identifica o arquivo nas mensagens. Sem `encoding`, o texto é
    decodificado pela code page declarada no cabeçalho (LDID_ENCODINGS).
    Registros marcados como excluídos são descartados.

    O FTP do DATASUS não publica hash dos arquivos, então a integridade é conferida
    pela estrutura: o arquivo precisa ter exatamente os registros que o cabeçalho
    declara (no máximo seguidos do byte 0x1A) e todo registro precisa começar com
    uma marca de exclusão válida, o que denuncia registros desalinhados.

    Raises:
        DBFFormatError: tamanho do arquivo diferente do declarado no cabeçalho,
            registros desalinhados ou campos inconsistentes.
        KeyError: alguma coluna pedida não existe no arquivo.
    """
    return _read(np.frombuffer(data, dtype=np.uint8), name, columns, encoding)


def _read(
    raw: np.ndarray,
    name: str,
    columns: Iterable[str] | None,
    encoding: str | None,
) -> pd.DataFrame:
    if raw.size < _HEADER_PREFIX_SIZE:
        raise DBFFormatError(f"{name}: arquivo com {raw.size} bytes, pequeno demais para um DBF")
    encoding = encoding or _header_encoding(raw, name)

    n_records, header_len, record_len = struct.unpack("<IHH", raw[4:12].tobytes())
    fields = _parse_fields(raw, header_len, record_len)

    data_end = header_len + n_records * record_len
    if raw.size < data_end:
        raise DBFFormatError(
            f"{name}: truncado ({raw.size:,} bytes; o cabeçalho declara {data_end:,})"
        )
    trailing = raw[data_end:]
    if trailing.size > 1 or (trailing.size == 1 and trailing[0] != _EOF_MARKER):
        # Bytes a mais significam registros fora da contagem do cabeçalho, que seriam perdidos
        raise DBFFormatError(
            f"{name}: {trailing.size:,} byte(s) além dos {n_records:,} registros declarados no cabeçalho"
        )

    if columns is not None:
        wanted = set(columns)
        missing = wanted - {f.name for f in fields}
        if missing:
            raise KeyError(f"{name}: colunas inexistentes: {sorted(missing)}")
        fields = [f for f in fields if f.name in wanted]

    # Checado só nas colunas pedidas: um memo no arquivo não impede ler as outras
    unsupported = [f"{f.name} ({f.type})" for f in fields if f.type not in SUPPORTED_TYPES]
    if unsupported:
        raise DBFFormatError(f"{name}: tipo de campo sem suporte: {', '.join(unsupported)}")

    records = raw[header_len:data_end].reshape(n_records, record_len)
    deleted = records[:, 0] == _DELETED_FLAG
    misaligned = int((~deleted & (records[:, 0] != _ACTIVE_FLAG)).sum())
    if misaligned:
        raise DBFFormatError(
            f"{name}: {misaligned:,} registro(s) sem marca de exclusão válida (arquivo corrompido?)"
        )
    if deleted.any():
        logger.info("%s: %d registro(s) marcado(s) como excluído(s) descartado(s)", name, deleted.sum())
        records = records[~deleted]

    return pd.DataFrame({f.name: _decode_field(records, f, encoding) for f in fields})
