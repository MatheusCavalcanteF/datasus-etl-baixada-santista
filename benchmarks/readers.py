"""Leitores comparados no benchmark. Cada função recebe o caminho do arquivo e devolve uma tabela.

As que terminam em _colunas leem só COLUMNS, as colunas que o transform usa.
As dbc_* recebem um .dbc e medem só a descompressão.
"""

from __future__ import annotations

import struct
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from src.dbf_reader import _parse_fields, read_dbf

COLUMNS = ["MUNIC_RES", "MUNIC_MOV", "DIAG_PRINC", "PROC_REA", "DT_INTER", "DT_SAIDA",
           "IDADE", "SEXO", "DIAS_PERM", "MORTE", "VAL_TOT", "CNES"]

# Leitor do PySUS (GPL-3.0), baixado na primeira execução em vez de copiado para o repositório
PYSUS_COMMIT = "b1be57342abdac43fa88fe0790576924edabb2cc"
PYSUS_URL = f"https://raw.githubusercontent.com/AlertaDengue/PySUS/{PYSUS_COMMIT}/pysus/data/dbf_reader.py"
CACHE_DIR = Path(__file__).parent / ".cache"


def _pysus():
    import importlib.util

    path = CACHE_DIR / f"pysus_dbf_reader_{PYSUS_COMMIT[:7]}.py"
    if not path.exists():
        CACHE_DIR.mkdir(exist_ok=True)
        urllib.request.urlretrieve(PYSUS_URL, path)
    spec = importlib.util.spec_from_file_location("pysus_dbf_reader", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- leitura de .dbf ---

def projeto(p):
    return read_dbf(Path(p))


def projeto_colunas(p):
    return read_dbf(Path(p), columns=COLUMNS)


def pysus(p):
    return _pysus().read_dbf_fast(p)


def pysus_colunas(p):
    return _pysus().read_dbf_fast(p, columns=COLUMNS)


def dbfread(p):
    from dbfread import DBF
    return pd.DataFrame(iter(DBF(p, encoding="cp850")))


def simpledbf(p):
    from simpledbf import Dbf5
    return Dbf5(p, codec="cp850").to_dataframe()


def dbf_furman(p):
    import dbf
    table = dbf.Table(p, codepage="cp850")
    table.open(dbf.READ_ONLY)
    try:
        return pd.DataFrame([tuple(r) for r in table], columns=table.field_names)
    finally:
        table.close()


def pyogrio(p):
    import pyogrio as po
    return po.read_arrow(p)[1]


def duckdb_spatial(p):
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    rel = con.sql(f"SELECT * FROM ST_Read('{Path(p).as_posix()}')")
    return rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table()


def fastdbf(p):
    # Atenção: nos .dbf do DATASUS a saída vem errada (lê todos os campos a partir do byte 0
    # do registro). Fica aqui só para mostrar que velocidade sem conferência de saída engana.
    import fastdbf as fd
    with fd.Table(p).open("r") as table:
        return pa.table(table.to_arrow())


# --- protótipo: mesma leitura de bytes do projeto, texto decodificado em Arrow em vez de str do Python ---

def _arrow_text(col: np.ndarray, encoding: str) -> pa.Array:
    m, width = col.shape
    if not (col >= 0x80).any():
        # ASCII puro também é UTF-8 válido: vira string Arrow sem passar por objetos Python
        offsets = np.arange(0, (m + 1) * width, width, dtype=np.int32)
        arr = pa.Array.from_buffers(pa.string(), m, [None, pa.py_buffer(offsets), pa.py_buffer(col)])
    else:
        arr = pa.array([v.decode(encoding) for v in col.view(f"S{width}").ravel()], pa.string())
    return pc.utf8_trim(arr, " \x00")


def prototipo_arrow(p, columns=None, encoding="cp850"):
    raw = np.fromfile(p, dtype=np.uint8)
    n, header_len, record_len = struct.unpack("<IHH", raw[4:12].tobytes())
    fields = _parse_fields(raw, header_len, record_len)
    if columns:
        fields = [f for f in fields if f.name in set(columns)]
    records = raw[header_len:header_len + n * record_len].reshape(n, record_len)
    records = records[records[:, 0] != ord("*")]
    out = {}
    for f in fields:
        arr = _arrow_text(np.ascontiguousarray(records[:, f.offset:f.offset + f.length]), encoding)
        if f.type in ("N", "F"):
            arr = pc.if_else(pc.equal(arr, ""), pa.scalar(None, pa.string()), arr)
            try:
                arr = pc.cast(arr, pa.int64() if f.decimals == 0 else pa.float64())
            except pa.ArrowInvalid:
                arr = pc.cast(arr, pa.float64())
        elif f.type == "D":
            arr = pc.strptime(arr, format="%Y%m%d", unit="s", error_is_null=True)
        out[f.name] = arr
    return pa.table(out)


def prototipo_arrow_colunas(p):
    return prototipo_arrow(p, COLUMNS)


# --- descompressão de .dbc (o resultado é o .dbf em bytes) ---

def dbc_datasus_disco(p):
    import tempfile

    import datasus_dbc
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "out.dbf"
        datasus_dbc.decompress(str(p), str(out))
        return out.read_bytes()


def dbc_datasus_memoria(p):
    import datasus_dbc
    return datasus_dbc.decompress_bytes(Path(p).read_bytes())


def dbc_pyreaddbc(p):
    import tempfile

    from pyreaddbc import dbc2dbf
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "out.dbf"
        dbc2dbf(str(p), str(out))
        return out.read_bytes()


DBF_READERS = ["projeto", "projeto_colunas", "pysus", "pysus_colunas", "prototipo_arrow",
               "prototipo_arrow_colunas", "duckdb_spatial", "pyogrio", "simpledbf", "dbfread"]
DBC_READERS = ["dbc_datasus_memoria", "dbc_datasus_disco", "dbc_pyreaddbc"]
SLOW = {"dbfread", "simpledbf", "dbf_furman", "pyogrio", "dbc_datasus_disco"}
