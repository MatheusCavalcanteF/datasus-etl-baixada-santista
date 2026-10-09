"""Carga: trusted_internacoes.parquet -> Star Schema no SQLite (data/database/hospital_data.db).

Tabelas:
    Dim_Municipio    municípios de residência dos pacientes (e dos hospitais)
    Dim_Hospital     estabelecimentos (CNES), com o município onde ficam
    Dim_CID          diagnósticos principais (CID-10)
    Dim_Tempo        calendário contínuo em anos completos, como o Power BI exige
    Fato_Internacao  uma linha por AIH processada (ver src/transform.py)

Idempotência: cada execução apaga e recria as tabelas dentro de UMA transação,
com PK, FK e CHECK declarados, e só faz o commit depois das validações. Se algo
falhar, o banco continua exatamente como estava. O to_sql(if_exists="replace")
do pandas recriaria as tabelas sem chaves e faria commit tabela a tabela,
podendo deixar dimensões novas com um fato antigo.

O SQLite não tem tipo data: as datas são gravadas como texto ISO (AAAA-MM-DD)
em colunas declaradas DATE, para o driver ODBC poder reconhecê-las como data.

Uso, a partir da raiz do projeto:
    python -m src.load
"""

from __future__ import annotations

import logging
import math
import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import BAIXADA_SANTISTA, DATABASE_PATH, TRUSTED_DIR
from src.log_config import setup_logging
from src.transform import KEY_COLUMNS, TIPO_AIH_NORMAL

logger = logging.getLogger(__name__)

TRUSTED_PATH = TRUSTED_DIR / "trusted_internacoes.parquet"

REGIAO_BAIXADA = "Baixada Santista"
REGIAO_FORA = "Fora da Baixada"
UF_POR_CODIGO = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}
NOMES_MES = (
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
)

# Ordem de criação = ordem de carga: dimensões antes do fato, por causa das FKs.
SCHEMA: dict[str, str] = {
    "Dim_Municipio": """
        CREATE TABLE Dim_Municipio (
            id_municipio   TEXT PRIMARY KEY,
            nome_municipio TEXT,
            uf             TEXT NOT NULL,
            regiao         TEXT NOT NULL
        )""",
    "Dim_Hospital": """
        CREATE TABLE Dim_Hospital (
            id_hospital           TEXT PRIMARY KEY,
            nome_hospital         TEXT,
            id_municipio_hospital TEXT NOT NULL,
            municipio_hospital    TEXT NOT NULL
        )""",
    "Dim_CID": """
        CREATE TABLE Dim_CID (
            id_cid        TEXT PRIMARY KEY,
            descricao_cid TEXT
        )""",
    "Dim_Tempo": """
        CREATE TABLE Dim_Tempo (
            data      DATE PRIMARY KEY,
            ano       INTEGER NOT NULL,
            trimestre INTEGER NOT NULL,
            mes       INTEGER NOT NULL,
            nome_mes  TEXT NOT NULL,
            ano_mes   TEXT NOT NULL,
            dia       INTEGER NOT NULL
        )""",
    "Fato_Internacao": """
        CREATE TABLE Fato_Internacao (
            id_internacao           INTEGER PRIMARY KEY,
            id_aih                  TEXT NOT NULL,
            competencia             DATE NOT NULL REFERENCES Dim_Tempo (data),
            tipo_aih                TEXT NOT NULL,
            id_hospital             TEXT NOT NULL REFERENCES Dim_Hospital (id_hospital),
            id_municipio_residencia TEXT REFERENCES Dim_Municipio (id_municipio),
            id_cid                  TEXT REFERENCES Dim_CID (id_cid),
            data_internacao         DATE NOT NULL REFERENCES Dim_Tempo (data),
            data_saida              DATE NOT NULL REFERENCES Dim_Tempo (data),
            dias_permanencia        INTEGER NOT NULL CHECK (dias_permanencia >= 0),
            valor_total             REAL NOT NULL,
            obito                   INTEGER NOT NULL CHECK (obito IN (0, 1)),
            desfecho                TEXT NOT NULL,
            especialidade_leito     TEXT,
            carater_internacao      TEXT NOT NULL,
            sexo                    TEXT NOT NULL,
            idade_anos              INTEGER,
            UNIQUE (id_aih, competencia, tipo_aih, data_saida)
        )""",
}

FATO_COLUMNS = [
    "id_internacao", "id_aih", "competencia", "tipo_aih", "id_hospital", "id_municipio_residencia",
    "id_cid", "data_internacao", "data_saida", "dias_permanencia", "valor_total", "obito",
    "desfecho", "especialidade_leito", "carater_internacao", "sexo", "idade_anos",
]
DATE_COLUMNS = ("competencia", "data_internacao", "data_saida")


class LoadValidationError(Exception):
    """O banco carregado não confere com o Parquet trusted; a transação é desfeita."""


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def read_trusted(path: Path = TRUSTED_PATH) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{path} não existe; rode antes: python -m src.transform")
    df = pd.read_parquet(path)
    logger.info("Trusted lido: %s linha(s) de %s", _fmt(len(df)), path.name)
    return df


def build_dim_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """Todos os municípios de residência e de hospital presentes no fato.

    Só os 9 da Baixada têm nome (config.py); os demais ficam sem nome, mas com UF
    e região, para não perder as AIHs de pacientes de fora no Power BI.
    """
    codes = pd.concat([df["id_municipio_residencia"], df["id_municipio"]]).dropna().unique()
    dim = pd.DataFrame({"id_municipio": sorted(codes)})
    dim["nome_municipio"] = dim["id_municipio"].map(BAIXADA_SANTISTA)
    dim["uf"] = dim["id_municipio"].str[:2].map(UF_POR_CODIGO).fillna("Ignorado")
    in_baixada = dim["id_municipio"].isin(list(BAIXADA_SANTISTA))
    dim["regiao"] = in_baixada.map({True: REGIAO_BAIXADA, False: REGIAO_FORA})
    return dim


def build_dim_hospital(df: pd.DataFrame) -> pd.DataFrame:
    """Um CNES por linha. nome_hospital fica nulo até integrarmos a API do CNES."""
    locations = df[["id_hospital", "id_municipio"]].drop_duplicates()
    multi = locations["id_hospital"].duplicated(keep=False)
    if multi.any():
        logger.warning(
            "%d hospital(is) com mais de um município; mantido o mais frequente",
            locations.loc[multi, "id_hospital"].nunique(),
        )
        locations = (
            df.groupby(["id_hospital", "id_municipio"]).size().rename("n").reset_index()
            .sort_values("n", ascending=False).drop_duplicates("id_hospital")
        )
    dim = locations.rename(columns={"id_municipio": "id_municipio_hospital"}).sort_values("id_hospital")
    dim["nome_hospital"] = None
    dim["municipio_hospital"] = dim["id_municipio_hospital"].map(BAIXADA_SANTISTA)
    return dim[["id_hospital", "nome_hospital", "id_municipio_hospital", "municipio_hospital"]]


def build_dim_cid(df: pd.DataFrame) -> pd.DataFrame:
    """CIDs distintos. descricao_cid fica nula até integrarmos a tabela CID-10 do DATASUS."""
    dim = pd.DataFrame({"id_cid": sorted(df["id_cid"].dropna().unique())})
    dim["descricao_cid"] = None
    return dim


def build_dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    """Calendário diário sem lacunas, de 1º/jan do menor ano a 31/dez do maior ano.

    Cobre todas as colunas de data do fato, não só data_saida, que tem dias sem
    nenhuma alta. O Power BI exige datas contínuas para marcar a tabela de datas
    e para funções como SAMEPERIODLASTYEAR.
    """
    first = min(df[col].min() for col in DATE_COLUMNS)
    last = max(df[col].max() for col in DATE_COLUMNS)
    dates = pd.Series(pd.date_range(f"{first.year}-01-01", f"{last.year}-12-31", freq="D"))
    return pd.DataFrame(
        {
            "data": dates.dt.strftime("%Y-%m-%d"),
            "ano": dates.dt.year,
            "trimestre": dates.dt.quarter,
            "mes": dates.dt.month,
            "nome_mes": dates.dt.month.map(lambda m: NOMES_MES[m - 1]),
            "ano_mes": dates.dt.strftime("%Y-%m"),
            "dia": dates.dt.day,
        }
    )


def build_fato_internacao(df: pd.DataFrame) -> pd.DataFrame:
    """Fato com chave substituta sequencial, na ordem da chave natural (determinística entre execuções)."""
    fato = df.sort_values(KEY_COLUMNS).reset_index(drop=True)
    fato.insert(0, "id_internacao", range(1, len(fato) + 1))
    for col in DATE_COLUMNS:
        fato[col] = fato[col].dt.strftime("%Y-%m-%d")
    fato["obito"] = fato["obito"].astype("int64")
    return fato[FATO_COLUMNS]


def _to_rows(df: pd.DataFrame) -> list[tuple[Any, ...]]:
    """Converte para tipos nativos do Python: o sqlite3 não aceita numpy.int64 nem pd.NA."""
    columns = [col.astype(object).where(col.notna(), None).tolist() for _, col in df.items()]
    return list(zip(*columns))


def _insert(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    # Nomes de tabela e colunas vêm das constantes deste módulo, nunca de entrada externa
    columns = ", ".join(df.columns)
    placeholders = ", ".join("?" * df.shape[1])
    conn.executemany(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", _to_rows(df))
    logger.info("%s: %s linha(s) inserida(s)", table, _fmt(len(df)))
    return len(df)


def recreate_schema(conn: sqlite3.Connection) -> None:
    for table in reversed(SCHEMA):  # fato antes das dimensões, por causa das FKs
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    for ddl in SCHEMA.values():
        conn.execute(ddl)


def validate_load(conn: sqlite3.Connection, trusted: pd.DataFrame) -> None:
    """Confere o banco contra o trusted antes do commit.

    Raises:
        LoadValidationError: lista todas as divergências encontradas.
    """
    (normal,) = conn.execute(
        "SELECT COUNT(*) FROM Fato_Internacao WHERE tipo_aih = ?", (TIPO_AIH_NORMAL,)
    ).fetchone()
    expected_normal = int((trusted["tipo_aih"] == TIPO_AIH_NORMAL).sum())
    logger.info(
        "Validação: SELECT COUNT(*) FROM Fato_Internacao WHERE tipo_aih = '%s' -> %s (esperado pelo trusted: %s)",
        TIPO_AIH_NORMAL,
        _fmt(normal),
        _fmt(expected_normal),
    )

    rows, days, value = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(dias_permanencia), 0), COALESCE(SUM(valor_total), 0.0) FROM Fato_Internacao"
    ).fetchone()
    orphans = conn.execute("PRAGMA foreign_key_check").fetchall()

    problems: list[str] = []
    if normal != expected_normal:
        problems.append(f"internações: banco {normal} x trusted {expected_normal}")
    if rows != len(trusted):
        problems.append(f"linhas do fato: banco {rows} x trusted {len(trusted)}")
    if days != int(trusted["dias_permanencia"].sum()):
        problems.append(f"soma de dias: banco {days} x trusted {int(trusted['dias_permanencia'].sum())}")
    if not math.isclose(value, float(trusted["valor_total"].sum()), abs_tol=0.01):
        problems.append(f"soma de valores: banco {value:.2f} x trusted {trusted['valor_total'].sum():.2f}")
    if orphans:
        problems.append(f"{len(orphans)} chave(s) estrangeira(s) sem correspondência: {orphans[:5]}")
    if problems:
        raise LoadValidationError("; ".join(problems))

    logger.info(
        "Validação OK: %s linhas, %s dias de permanência e R$ %s no fato; nenhuma FK órfã",
        _fmt(rows),
        _fmt(days),
        f"{value:,.2f}".translate(str.maketrans(",.", ".,")),
    )


def load(trusted_path: Path = TRUSTED_PATH, db_path: Path = DATABASE_PATH) -> dict[str, int]:
    """Recria o Star Schema no SQLite. Retorna as linhas inseridas por tabela."""
    started = time.perf_counter()
    trusted = read_trusted(trusted_path)
    tables = {
        "Dim_Municipio": build_dim_municipio(trusted),
        "Dim_Hospital": build_dim_hospital(trusted),
        "Dim_CID": build_dim_cid(trusted),
        "Dim_Tempo": build_dim_tempo(trusted),
        "Fato_Internacao": build_fato_internacao(trusted),
    }
    logger.info("Início da carga em %s", db_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None: BEGIN/COMMIT explícitos, para que DROP e CREATE
    # também fiquem dentro da transação (o modo padrão do sqlite3 não os inclui)
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=30)
    try:
        conn.execute("PRAGMA foreign_keys = ON")  # precisa vir antes do BEGIN, senão é ignorado
        conn.execute("BEGIN IMMEDIATE")
        try:
            recreate_schema(conn)
            counts = {table: _insert(conn, table, frame) for table, frame in tables.items()}
            validate_load(conn, trusted)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            logger.error("Carga desfeita (ROLLBACK); o banco continua como antes desta execução")
            raise
    finally:
        conn.close()

    logger.info("Fim da carga em %.1fs: %s", time.perf_counter() - started, db_path)
    return counts


def main() -> int:
    setup_logging()
    try:
        load()
    except (FileNotFoundError, LoadValidationError, sqlite3.Error) as exc:
        logger.error("Carga terminou com erro: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
