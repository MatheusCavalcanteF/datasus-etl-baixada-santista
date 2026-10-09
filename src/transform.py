"""Transformação: AIH Reduzida (SIH/SUS) de SP -> internações na Baixada Santista.

Lê os Parquets RD gerados pelo extract (data/converted/), mantém só as AIHs de
hospitais da Baixada Santista (MUNIC_MOV), seleciona e renomeia as colunas,
tipa, limpa e grava tudo em data/trusted/trusted_internacoes.parquet.

Granularidade: uma linha por AIH processada. Uma internação de longa permanência
gera uma AIH por mês com o mesmo número (IDENT=5), cada uma só com os dias e o
valor daquele mês. O hospital pode apresentar duas dessas fatias na mesma
competência (ex.: abril atrasado junto com maio), então a chave é
(id_aih, competencia, tipo_aih, data_saida). Para contar internações, conte só
tipo_aih == "Normal"; o TabNet também não conta as AIHs de prorrogação.

Os rótulos de sexo, caráter, tipo de AIH e desfecho seguem as tabelas oficiais
do TabWin (TAB_SIH.zip: SEXO.CNV, CARATEND.CNV, IDENT.CNV, SAIDAPERM.CNV).

Uso, a partir da raiz do projeto:
    python -m src.transform
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.config import BAIXADA_SANTISTA, CONVERTED_DIR, TRUSTED_DIR
from src.log_config import setup_logging

logger = logging.getLogger(__name__)

SOURCE_FILE_PATTERN = re.compile(r"RD[A-Z]{2}\d{4}\.parquet")
OUTPUT_PATH = TRUSTED_DIR / "trusted_internacoes.parquet"

RENAME_MAP: dict[str, str] = {
    "N_AIH": "id_aih",
    "ANO_CMPT": "ano_competencia",
    "MES_CMPT": "mes_competencia",
    "IDENT": "tipo_aih",
    "CNES": "id_hospital",
    "MUNIC_MOV": "id_municipio",
    "MUNIC_RES": "id_municipio_residencia",
    "DIAG_PRINC": "id_cid",
    "DT_INTER": "data_internacao",
    "DT_SAIDA": "data_saida",
    "DIAS_PERM": "dias_permanencia",
    "VAL_TOT": "valor_total",
    "MORTE": "obito",
    "COBRANCA": "desfecho",
    "ESPEC": "especialidade_leito",
    "CAR_INT": "carater_internacao",
    "SEXO": "sexo",
    "IDADE": "idade",
    "COD_IDADE": "unidade_idade",
}
SOURCE_COLUMNS = list(RENAME_MAP)

OUTPUT_COLUMNS = [
    "id_aih",
    "competencia",
    "tipo_aih",
    "id_hospital",
    "id_municipio",
    "id_municipio_residencia",
    "id_cid",
    "data_internacao",
    "data_saida",
    "dias_permanencia",
    "valor_total",
    "obito",
    "desfecho",
    "especialidade_leito",  # código; rótulos em LEITOS.CNV (ex.: 09 = Leito Dia / Cirúrgicos)
    "carater_internacao",
    "sexo",
    "idade_anos",
]
KEY_COLUMNS = ["id_aih", "competencia", "tipo_aih", "data_saida"]
REQUIRED_COLUMNS = [
    "id_aih",
    "competencia",
    "tipo_aih",
    "id_hospital",
    "id_municipio",
    "data_internacao",
    "data_saida",
    "dias_permanencia",
    "valor_total",
    "obito",
]

UNKNOWN = "Ignorado"
TIPO_AIH_NORMAL = "Normal"
TIPO_AIH = {"1": TIPO_AIH_NORMAL, "5": "Longa permanência"}
SEXO = {"1": "Masculino", "2": "Feminino", "3": "Feminino"}
CARATER_INTERNACAO = {
    "01": "Eletivo",
    "02": "Urgência",
    "03": "Acidente de trabalho",
    "04": "Acidente de trajeto",
    "05": "Acidente de trânsito",
    "06": "Outras causas externas",
}
CID10_PATTERN = r"[A-Z]\d{2}[0-9A-Z]?"


class TrustedValidationError(Exception):
    """O DataFrame final viola alguma regra da camada trusted."""


def list_source_files(source_dir: Path = CONVERTED_DIR) -> list[Path]:
    """Lista os Parquets RD (ex.: RDSP2601.parquet), ignorando outros arquivos da pasta."""
    files = sorted(p for p in source_dir.glob("RD*.parquet") if SOURCE_FILE_PATTERN.fullmatch(p.name))
    if not files:
        raise FileNotFoundError(f"Nenhum Parquet RD em {source_dir}; rode antes: python -m src.extract")
    return files


def read_baixada(path: Path, municipios: Sequence[str] = tuple(BAIXADA_SANTISTA)) -> pd.DataFrame:
    """Lê só as colunas usadas e só as AIHs com hospital nos municípios informados."""
    missing = set(SOURCE_COLUMNS) - set(pq.read_schema(path).names)
    if missing:
        raise KeyError(f"{path.name}: colunas ausentes no Parquet: {sorted(missing)}")

    df = pd.read_parquet(path, columns=SOURCE_COLUMNS, filters=[("MUNIC_MOV", "in", list(municipios))])
    logger.info("%s: %s AIH(s) em hospitais da Baixada Santista", path.name, _fmt(len(df)))
    return df


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def _drop_rows(df: pd.DataFrame, mask: pd.Series, reason: str) -> pd.DataFrame:
    count = int(mask.sum())
    if count:
        logger.warning("%s linha(s) removida(s): %s", _fmt(count), reason)
    return df.loc[~mask]


def clean_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços dos códigos, descarta linhas sem identificador e anula CIDs mal formados."""
    df = df.copy()
    text_columns = [
        "id_aih", "ano_competencia", "mes_competencia", "tipo_aih", "id_hospital", "id_municipio",
        "id_municipio_residencia", "id_cid", "desfecho", "especialidade_leito",
        "carater_internacao", "sexo", "unidade_idade",
    ]
    for col in text_columns:
        df[col] = df[col].str.strip().replace("", pd.NA)

    missing_id = df[["id_aih", "id_hospital", "id_municipio"]].isna().any(axis=1)
    df = _drop_rows(df, missing_id, "sem número da AIH, CNES ou município")

    df["id_cid"] = df["id_cid"].str.upper()
    bad_cid = df["id_cid"].notna() & ~df["id_cid"].str.fullmatch(CID10_PATTERN).fillna(False)
    if bad_cid.any():
        logger.warning(
            "%s CID(s) fora do formato CID-10 viraram nulo: %s",
            _fmt(int(bad_cid.sum())),
            df.loc[bad_cid, "id_cid"].value_counts().head(5).to_dict(),
        )
        df.loc[bad_cid, "id_cid"] = pd.NA
    if df["id_cid"].isna().any():
        logger.warning("%s AIH(s) sem CID principal válido", _fmt(int(df["id_cid"].isna().sum())))
    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Converte AAAAMMDD em datetime, monta a competência (1º dia do mês) e descarta datas impossíveis."""
    df = df.copy()
    df["competencia"] = pd.to_datetime(
        df["ano_competencia"] + df["mes_competencia"], format="%Y%m", errors="coerce"
    )
    for col in ("data_internacao", "data_saida"):
        df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce")

    invalid = df[["competencia", "data_internacao", "data_saida"]].isna().any(axis=1)
    df = _drop_rows(df, invalid, "competência, data de internação ou de saída ausente ou inválida")
    df = _drop_rows(df, df["data_saida"] < df["data_internacao"], "data de saída anterior à internação")
    return df


def _classify_outcome(codes: pd.Series) -> pd.Series:
    """Agrupa o motivo de saída/permanência (COBRANCA) conforme SAIDAPERM.CNV."""
    codes = codes.fillna("")
    first_digit = codes.str[:1]
    conditions = [
        (first_digit == "4") | codes.isin(["65", "66", "67"]),
        (first_digit == "1") | codes.isin(["61", "62", "63", "64"]),
        codes.isin(["29", "31", "32"]),  # o 29 começa com 2, mas é transferência
        first_digit == "2",
        codes == "51",
    ]
    choices = ["Óbito", "Alta", "Transferência", "Permanência", "Encerramento administrativo"]
    return pd.Series(np.select(conditions, choices, default=UNKNOWN), index=codes.index, dtype="str")


def _age_in_years(unit: pd.Series, value: pd.Series) -> pd.Series:
    """COD_IDADE: 2 = dias, 3 = meses, 4 = anos, 5 = 100 + IDADE anos (IDADEDET.CNV)."""
    age = pd.Series(pd.NA, index=unit.index, dtype="Int64")
    age[unit.isin(["2", "3"])] = 0
    age[unit == "4"] = value[unit == "4"]
    age[unit == "5"] = value[unit == "5"] + 100
    return age


def decode_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Troca códigos do SIH por rótulos legíveis no Power BI."""
    df = df.copy()

    unknown_types = df.loc[~df["tipo_aih"].isin(list(TIPO_AIH)), "tipo_aih"]
    if not unknown_types.empty:
        logger.warning("Tipo de AIH (IDENT) inesperado: %s", unknown_types.value_counts(dropna=False).to_dict())
    df["tipo_aih"] = df["tipo_aih"].map(TIPO_AIH).fillna(UNKNOWN)
    df["sexo"] = df["sexo"].map(SEXO).fillna(UNKNOWN)
    df["carater_internacao"] = df["carater_internacao"].map(CARATER_INTERNACAO).fillna(UNKNOWN)
    df["desfecho"] = _classify_outcome(df["desfecho"])
    df["idade_anos"] = _age_in_years(df["unidade_idade"], df["idade"])
    return df


def clean_measures(df: pd.DataFrame) -> pd.DataFrame:
    """Garante dias_permanencia inteiro, valor_total float e obito booleano, sem nulos."""
    df = df.copy()

    # Nulo vira 0 só quando 0 é o sentido certo. Para dias, 0 derrubaria a média de
    # permanência; o valor sai das datas, que batem com DIAS_PERM em 99,9% das AIHs
    # normais. Nas de longa permanência, DT_INTER é a entrada original, e as datas não servem.
    missing_days = df["dias_permanencia"].isna()
    if missing_days.any():
        derivable = missing_days & (df["tipo_aih"] == TIPO_AIH_NORMAL)
        if derivable.any():
            df.loc[derivable, "dias_permanencia"] = (df["data_saida"] - df["data_internacao"]).dt.days[derivable]
            logger.warning("%s dias_permanencia nulo(s) recalculado(s) pelas datas", _fmt(int(derivable.sum())))
        df = _drop_rows(df, missing_days & ~derivable, "AIH de longa permanência sem dias_permanencia")
    df = _drop_rows(df, df["dias_permanencia"] < 0, "dias_permanencia negativo")
    df["dias_permanencia"] = df["dias_permanencia"].astype("int64")

    # VAL_TOT = 0 é legítimo (ex.: leitos de saúde mental, ESPEC 87, custeados fora da AIH)
    missing_value = df["valor_total"].isna()
    if missing_value.any():
        logger.warning("%s valor_total nulo(s) preenchido(s) com 0", _fmt(int(missing_value.sum())))
    df["valor_total"] = df["valor_total"].fillna(0.0).astype("float64")
    if (df["valor_total"] < 0).any():
        logger.warning("%s AIH(s) com valor_total negativo", _fmt(int((df["valor_total"] < 0).sum())))

    missing_death = df["obito"].isna()
    if missing_death.any():
        logger.warning("%s obito nulo(s) considerado(s) como não óbito", _fmt(int(missing_death.sum())))
    df["obito"] = df["obito"].fillna(0).eq(1).astype("bool")
    return df


def validate_trusted(df: pd.DataFrame) -> None:
    """Confere as regras da camada trusted antes de gravar.

    Raises:
        TrustedValidationError: lista todas as violações encontradas.
    """
    problems: list[str] = []

    nulls = df[REQUIRED_COLUMNS].isna().sum()
    nulls = nulls[nulls > 0]
    if not nulls.empty:
        problems.append(f"nulos em colunas obrigatórias: {nulls.to_dict()}")

    duplicates = int(df.duplicated(KEY_COLUMNS).sum())
    if duplicates:
        problems.append(f"{duplicates} linha(s) repetida(s) na chave {KEY_COLUMNS}")

    outside = ~df["id_municipio"].isin(list(BAIXADA_SANTISTA))
    if outside.any():
        problems.append(f"{int(outside.sum())} linha(s) fora da Baixada Santista")

    if (df["dias_permanencia"] < 0).any():
        problems.append("dias_permanencia negativo")

    if problems:
        raise TrustedValidationError("; ".join(problems))


def save_trusted(df: pd.DataFrame, output_path: Path = OUTPUT_PATH) -> Path:
    """Grava o Parquet via arquivo .part, para não deixar um trusted pela metade se algo falhar."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = output_path.with_name(output_path.name + ".part")
    try:
        df.to_parquet(part_path, index=False)
        part_path.replace(output_path)
    finally:
        part_path.unlink(missing_ok=True)
    return output_path


def transform(source_dir: Path = CONVERTED_DIR, output_path: Path = OUTPUT_PATH) -> pd.DataFrame:
    """Executa a transformação completa e devolve o DataFrame gravado."""
    started = time.perf_counter()
    files = list_source_files(source_dir)
    logger.info("Início da transformação: %d arquivo(s) RD em %s", len(files), source_dir)

    raw = pd.concat([read_baixada(path) for path in files], ignore_index=True)
    logger.info("Total lido: %s AIH(s) da Baixada Santista", _fmt(len(raw)))

    df = raw.rename(columns=RENAME_MAP)
    df = clean_identifiers(df)
    df = parse_dates(df)
    df = decode_categories(df)
    df = clean_measures(df)
    df = df[OUTPUT_COLUMNS].sort_values(KEY_COLUMNS).reset_index(drop=True)

    validate_trusted(df)
    save_trusted(df, output_path)

    by_month = df.groupby(df["competencia"].dt.strftime("%Y-%m")).size()
    logger.info("AIHs por competência: %s", ", ".join(f"{m}: {_fmt(n)}" for m, n in by_month.items()))
    logger.info(
        "Fim da transformação em %.1fs: %s AIH(s) x %d colunas gravadas em %s (%s descartada(s)); "
        "%s internação(ões), sem contar as AIHs de prorrogação",
        time.perf_counter() - started,
        _fmt(len(df)),
        df.shape[1],
        output_path,
        _fmt(len(raw) - len(df)),
        _fmt(int((df["tipo_aih"] == TIPO_AIH_NORMAL).sum())),
    )
    return df


def main() -> int:
    setup_logging()
    try:
        transform()
    except (FileNotFoundError, KeyError, TrustedValidationError) as exc:
        logger.error("Transformação terminou com erro: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
