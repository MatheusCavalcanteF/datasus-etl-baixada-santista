"""Dados de referência: nomes dos hospitais (API do CNES) e tabela CID-10 do DATASUS.

Gera em data/reference/:
    cnes_estabelecimentos.parquet  um CNES por linha: nome fantasia, razão social e município
    cid10.parquet                  subcategorias e categorias da CID-10, com categoria e capítulo

O load usa esses arquivos para preencher Dim_Hospital.nome_hospital e as colunas
de descrição da Dim_CID. Sem eles, o load continua e deixa esses campos nulos.

- CNES: a API de dados abertos do Ministério da Saúde devolve um estabelecimento
  por consulta (~0,2 s). Só os CNES que ainda não estão no cache são consultados,
  então uma nova execução sem hospitais novos não faz nenhuma requisição.
- CID-10: a tabela do DATASUS (versão 2008) só é publicada por HTTP, sem TLS. Como
  o arquivo não muda desde 2007, o download é conferido pelo SHA-256.

Rodar depois do transform (os CNES saem do trusted) e antes do load:
    python -m src.reference
    python -m src.reference --forcar   # consulta e baixa tudo de novo
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import REFERENCE_DIR, TRUSTED_DIR
from src.log_config import setup_logging

logger = logging.getLogger(__name__)

TRUSTED_PATH = TRUSTED_DIR / "trusted_internacoes.parquet"
HOSPITALS_PATH = REFERENCE_DIR / "cnes_estabelecimentos.parquet"
CID10_PATH = REFERENCE_DIR / "cid10.parquet"

CNES_API_URL = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/{cnes}"
CID10_URL = "http://www2.datasus.gov.br/cid10/V2008/downloads/CID10CSV.zip"
CID10_SHA256 = "84f23809275575f751255048064bbb244b0de33fd5987ab98df0f98e5f5d2c95"
CID10_ENCODING = "latin-1"  # conferido: nenhum byte 0x80-0x9F, então latin-1 e cp1252 dão o mesmo texto

HTTP_TIMEOUT_S = 30
MAX_ATTEMPTS = 4
BACKOFF_BASE_S = 2  # espera 2s, 4s e 8s entre as tentativas
MAX_NAME_LENGTH = 200  # a resposta da API é dado externo: nome maior que isso é recusado

HOSPITAL_COLUMNS = ["id_hospital", "nome_hospital", "razao_social", "id_municipio_cnes", "consultado_em_utc"]
CID10_COLUMNS = ["id_cid", "descricao_cid", "id_categoria", "descricao_categoria", "numero_capitulo", "capitulo"]


class NotFoundError(Exception):
    """O servidor respondeu 404."""


class ReferenceDataError(Exception):
    """Um ou mais dados de referência não puderam ser obtidos ou validados."""


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def http_get(url: str) -> bytes:
    """GET com novas tentativas em falhas de rede e erros 5xx.

    Raises:
        NotFoundError: o servidor respondeu 404 (sem nova tentativa).
        urllib.error.URLError | TimeoutError: falha persistente depois de MAX_ATTEMPTS tentativas.
    """
    request = urllib.request.Request(url, headers={"User-Agent": "datasus-etl-baixada-santista"})
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise NotFoundError(url) from exc
            if exc.code < 500 or attempt == MAX_ATTEMPTS:
                raise
            error: Exception = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise
            error = exc
        wait_s = BACKOFF_BASE_S * 2 ** (attempt - 1)
        logger.warning("Falha em %s (tentativa %d/%d): %s. Nova tentativa em %ds", url, attempt, MAX_ATTEMPTS, error, wait_s)
        time.sleep(wait_s)
    raise AssertionError("inalcançável: o laço sempre retorna ou propaga a exceção")


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Grava via .part, para não deixar um arquivo de referência pela metade."""
    path.parent.mkdir(parents=True, exist_ok=True)
    part_path = path.with_name(path.name + ".part")
    try:
        df.to_parquet(part_path, index=False)
        part_path.replace(path)
    finally:
        part_path.unlink(missing_ok=True)


# --- CNES ---

def _clean_name(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    name = " ".join(value.split())
    if not name:
        return None
    if len(name) > MAX_NAME_LENGTH:
        raise ValueError(f"nome com {len(name)} caracteres")
    return name


def fetch_hospital(cnes: str) -> dict[str, str | None] | None:
    """Consulta um estabelecimento na API do CNES. Devolve None se o CNES não existir lá.

    O nome do painel é o nome fantasia (ex.: HOSPITAL REGIONAL JORGE ROSSMANN DE
    ITANHAEM); sem ele, fica a razão social. A API grava os nomes em maiúsculas e
    sem acento, e eles são mantidos assim: siglas como UPA e AME não sobreviveriam
    a uma conversão automática para "Título".

    Raises:
        ReferenceDataError: a resposta não é do CNES pedido ou não tem nome.
    """
    try:
        payload = json.loads(http_get(CNES_API_URL.format(cnes=cnes)))
    except NotFoundError:
        logger.warning("CNES %s não existe na API de dados abertos", cnes)
        return None

    if not isinstance(payload, dict) or str(payload.get("codigo_cnes", "")).zfill(7) != cnes:
        raise ReferenceDataError(f"CNES {cnes}: a API devolveu outro estabelecimento ({str(payload)[:80]})")
    try:
        fantasia = _clean_name(payload.get("nome_fantasia"))
        razao_social = _clean_name(payload.get("nome_razao_social"))
    except ValueError as exc:
        raise ReferenceDataError(f"CNES {cnes}: {exc}") from None
    if fantasia is None and razao_social is None:
        raise ReferenceDataError(f"CNES {cnes}: a API não devolveu nome fantasia nem razão social")

    municipio = payload.get("codigo_municipio")
    return {
        "id_hospital": cnes,
        "nome_hospital": fantasia or razao_social,
        "razao_social": razao_social,
        "id_municipio_cnes": str(municipio) if municipio is not None else None,
        "consultado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def read_hospitals(path: Path = HOSPITALS_PATH) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=HOSPITAL_COLUMNS)
    return pd.read_parquet(path)


def update_hospitals(cnes_codes: Iterable[str], path: Path = HOSPITALS_PATH, force: bool = False) -> pd.DataFrame:
    """Consulta na API os CNES que ainda não estão no cache e grava o cache atualizado.

    Um CNES com falha não interrompe os demais: os que deram certo são gravados,
    e as falhas são levantadas no final.

    Raises:
        ReferenceDataError: algum CNES falhou depois das novas tentativas.
    """
    wanted = sorted(set(cnes_codes))
    cache = pd.DataFrame(columns=HOSPITAL_COLUMNS) if force else read_hospitals(path)
    missing = [cnes for cnes in wanted if cnes not in set(cache["id_hospital"])]
    logger.info("CNES: %d no trusted, %d já no cache, %d a consultar", len(wanted), len(wanted) - len(missing), len(missing))

    fetched: list[dict[str, str | None]] = []
    failed: list[str] = []
    for cnes in missing:
        try:
            hospital = fetch_hospital(cnes)
        except (ReferenceDataError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            logger.error("Falha ao consultar o CNES %s: %s", cnes, exc)
            failed.append(cnes)
            continue
        if hospital is not None:
            fetched.append(hospital)
            logger.info("CNES %s: %s", cnes, hospital["nome_hospital"])

    if fetched or force:
        frames = [frame for frame in (cache, pd.DataFrame(fetched, columns=HOSPITAL_COLUMNS)) if not frame.empty]
        cache = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=HOSPITAL_COLUMNS)
        cache = cache.sort_values("id_hospital").reset_index(drop=True)
        _write_parquet(cache, path)
    if failed:
        raise ReferenceDataError(f"CNES sem resposta válida: {', '.join(failed)}")
    return cache


# --- CID-10 ---

def _read_csv(archive: zipfile.ZipFile, name: str) -> pd.DataFrame:
    with archive.open(name) as fh:
        df = pd.read_csv(fh, sep=";", encoding=CID10_ENCODING, dtype=str, keep_default_na=False)
    return df.loc[:, ~df.columns.str.startswith("Unnamed")]  # as linhas terminam com ";"


def parse_cid10_zip(data: bytes) -> pd.DataFrame:
    """Uma linha por código (subcategorias de 4 caracteres e categorias de 3), com categoria e capítulo."""
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        subcategories = _read_csv(archive, "CID-10-SUBCATEGORIAS.CSV")
        categories = _read_csv(archive, "CID-10-CATEGORIAS.CSV")
        chapters = _read_csv(archive, "CID-10-CAPITULOS.CSV")

    categories = categories.rename(columns={"CAT": "id_categoria", "DESCRICAO": "descricao_categoria"})
    codes = pd.concat(
        [
            subcategories.rename(columns={"SUBCAT": "id_cid", "DESCRICAO": "descricao_cid"})[["id_cid", "descricao_cid"]],
            categories.rename(columns={"id_categoria": "id_cid", "descricao_categoria": "descricao_cid"})[["id_cid", "descricao_cid"]],
        ],
        ignore_index=True,
    ).drop_duplicates("id_cid")
    codes["id_categoria"] = codes["id_cid"].str[:3]
    codes = codes.merge(categories[["id_categoria", "descricao_categoria"]], on="id_categoria", how="left")

    # Capítulo pela faixa de categorias (ex.: J00-J99 = Capítulo X); comparação de texto funciona com 3 caracteres
    codes["numero_capitulo"] = pd.Series(pd.NA, index=codes.index, dtype="Int64")
    codes["capitulo"] = pd.Series(pd.NA, index=codes.index, dtype="str")
    for chapter in chapters.itertuples(index=False):
        in_chapter = codes["id_categoria"].between(chapter.CATINIC, chapter.CATFIM)
        codes.loc[in_chapter, "numero_capitulo"] = int(chapter.NUMCAP)
        codes.loc[in_chapter, "capitulo"] = chapter.DESCRICAO.strip()

    without_chapter = codes["capitulo"].isna()
    if without_chapter.any():
        logger.warning("%d código(s) CID-10 fora das faixas de capítulo", int(without_chapter.sum()))
    return codes[CID10_COLUMNS].sort_values("id_cid").reset_index(drop=True)


def update_cid10(path: Path = CID10_PATH, force: bool = False) -> pd.DataFrame:
    """Baixa a tabela CID-10 do DATASUS, confere o SHA-256 e grava o Parquet.

    Raises:
        ReferenceDataError: o arquivo baixado não é o esperado.
    """
    if path.exists() and not force:
        logger.info("%s já existe; download da CID-10 ignorado", path.name)
        return pd.read_parquet(path)

    logger.info("Baixando a tabela CID-10 de %s", CID10_URL)
    data = http_get(CID10_URL)
    digest = hashlib.sha256(data).hexdigest()
    if digest != CID10_SHA256:
        # O servidor não tem TLS: conteúdo diferente do conhecido pode ser adulteração ou troca de versão
        raise ReferenceDataError(f"CID10CSV.zip com SHA-256 inesperado: {digest} (esperado {CID10_SHA256})")
    cid10 = parse_cid10_zip(data)
    _write_parquet(cid10, path)
    logger.info("CID-10: %s código(s) gravado(s) em %s", _fmt(len(cid10)), path)
    return cid10


def update_reference(trusted_path: Path = TRUSTED_PATH, reference_dir: Path = REFERENCE_DIR, force: bool = False) -> None:
    """Atualiza a CID-10 e os nomes dos hospitais do trusted.

    Raises:
        FileNotFoundError: o trusted ainda não existe.
        ReferenceDataError: alguma das duas fontes falhou (a outra é gravada mesmo assim).
    """
    if not trusted_path.exists():
        raise FileNotFoundError(f"{trusted_path} não existe; rode antes: python -m src.transform")
    cnes_codes = pd.read_parquet(trusted_path, columns=["id_hospital"])["id_hospital"].dropna().unique()

    errors: list[str] = []
    for name, step in (
        ("CID-10", lambda: update_cid10(reference_dir / CID10_PATH.name, force)),
        ("CNES", lambda: update_hospitals(cnes_codes, reference_dir / HOSPITALS_PATH.name, force)),
    ):
        try:
            step()
        except (ReferenceDataError, urllib.error.URLError, TimeoutError) as exc:
            logger.error("%s: %s", name, exc)
            errors.append(name)
    if errors:
        raise ReferenceDataError(f"falha em: {', '.join(errors)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Baixa os nomes dos hospitais (CNES) e a tabela CID-10.")
    parser.add_argument("--forcar", action="store_true", help="consulta e baixa tudo de novo, ignorando o cache")
    args = parser.parse_args(argv)
    setup_logging()
    started = time.perf_counter()
    try:
        update_reference(force=args.forcar)
    except (FileNotFoundError, ReferenceDataError) as exc:
        logger.error("Dados de referência terminaram com erro: %s", exc)
        return 1
    logger.info("Dados de referência atualizados em %.1fs", time.perf_counter() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
