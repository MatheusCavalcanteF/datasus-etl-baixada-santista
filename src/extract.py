"""Extração da AIH Reduzida (RD) do SIH/SUS a partir do FTP do DATASUS.

Baixa os arquivos RD{UF}{AAMM}.dbc para data/raw/ e converte cada um para
Parquet em data/converted/, com todas as colunas e linhas originais. O recorte
da Baixada Santista fica para o transform.

O DATASUS republica os últimos ~12 meses todo mês. Por isso cada arquivo só é
baixado de novo quando o tamanho ou a data de modificação no FTP mudam, e só é
convertido de novo quando o Parquet não corresponde ao .dbc atual.

Por padrão o período vai de janeiro do ano anterior até o mês atual. Os meses
que ainda não saíram são pulados com um aviso e entram sozinhos na primeira
execução depois de publicados.

Uso, a partir da raiz do projeto:
    python -m src.extract                                # ano anterior + ano atual
    python -m src.extract --inicio 2024-01 --fim 2024-12
    python -m src.extract --forcar                       # ignora o cache local
"""

from __future__ import annotations

import argparse
import ftplib
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import datasus_dbc
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import CONVERTED_DIR, RAW_DIR
from src.dbf_reader import read_dbf
from src.log_config import setup_logging

logger = logging.getLogger(__name__)

FTP_HOST = "ftp.datasus.gov.br"
SIH_DATA_DIR = "/dissemin/publicos/SIHSUS/200801_/Dados"
SIH_FIRST_MONTH = date(2008, 1, 1)  # a pasta 200801_ começa em jan/2008
FTP_TIMEOUT_S = 120
MAX_ATTEMPTS = 4
BACKOFF_BASE_S = 5  # espera 5s, 10s e 20s entre as tentativas
DBF_ENCODING = "latin-1"

DEFAULT_UF = "SP"


class IncompleteDownloadError(Exception):
    """O arquivo baixado tem tamanho diferente do informado pelo FTP."""


class ConversionError(Exception):
    """O Parquet gravado não tem as mesmas linhas lidas do .dbf."""


class RemoteDirectoryError(Exception):
    """O diretório esperado não existe no FTP (o DATASUS reorganizou as pastas?)."""


class ExtractionError(Exception):
    """Um ou mais arquivos falharam depois de esgotadas as tentativas."""


@dataclass(frozen=True)
class RemoteFile:
    name: str
    size: int
    modified: datetime  # UTC, obtido via MDTM


def default_period(today: date | None = None) -> tuple[date, date]:
    """Escopo do projeto: o ano anterior inteiro e o ano atual até o mês corrente."""
    today = today or date.today()
    return date(today.year - 1, 1, 1), date(today.year, today.month, 1)


def month_range(start: date, end: date) -> list[date]:
    """Primeiro dia de cada mês de start a end, inclusive."""
    if start > end:
        raise ValueError(f"Início ({start:%Y-%m}) depois do fim ({end:%Y-%m})")
    if start < SIH_FIRST_MONTH:
        raise ValueError(f"O SIH no formato atual começa em {SIH_FIRST_MONTH:%Y-%m}; recebido {start:%Y-%m}")
    months: list[date] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def sih_rd_filename(uf: str, competencia: date) -> str:
    """Monta o nome do arquivo RD, ex.: ('SP', date(2026, 1, 1)) -> 'RDSP2601.dbc'."""
    uf = uf.upper()
    if len(uf) != 2 or not uf.isalpha():
        raise ValueError(f"UF inválida: {uf!r}")
    return f"RD{uf}{competencia:%y%m}.dbc"


def _connect(remote_dir: str) -> ftplib.FTP:
    ftp = ftplib.FTP(FTP_HOST, timeout=FTP_TIMEOUT_S)
    try:
        ftp.login()
        try:
            ftp.cwd(remote_dir)
        except ftplib.error_perm as exc:
            # Sem isso, um 550 aqui seria confundido com "arquivo ainda não publicado"
            raise RemoteDirectoryError(f"Diretório {remote_dir} inacessível no FTP: {exc}") from exc
        ftp.voidcmd("TYPE I")  # SIZE só é confiável em modo binário
    except BaseException:
        ftp.close()
        raise
    return ftp


def _remote_info(ftp: ftplib.FTP, filename: str) -> RemoteFile:
    size = ftp.size(filename)
    if size is None:
        raise ftplib.error_reply(f"O servidor não informou o tamanho de {filename}")
    reply = ftp.voidcmd(f"MDTM {filename}")  # "213 AAAAMMDDHHMMSS"
    modified = datetime.strptime(reply.split()[1][:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return RemoteFile(filename, size, modified)


def _is_cached(local_path: Path, remote: RemoteFile) -> bool:
    if not local_path.exists():
        return False
    stat = local_path.stat()
    return stat.st_size == remote.size and int(stat.st_mtime) == int(remote.modified.timestamp())


def download_file(
    filename: str,
    remote_dir: str,
    dest_dir: Path,
    force: bool = False,
) -> Path | None:
    """Baixa um arquivo do FTP do DATASUS, com novas tentativas em falhas de rede.

    O arquivo é gravado primeiro como .part e só ganha o nome final depois de
    conferido o tamanho. A data de modificação local recebe a do FTP, o que
    permite detectar republicações nas execuções seguintes.

    Returns:
        Caminho local do arquivo, ou None se ele não existe no FTP (ex.: mês
        ainda não publicado).

    Raises:
        ftplib.Error | OSError | EOFError | IncompleteDownloadError: falha
            persistente depois de MAX_ATTEMPTS tentativas.
        RemoteDirectoryError: remote_dir não existe no FTP (sem nova tentativa).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    local_path = dest_dir / filename
    part_path = local_path.with_name(local_path.name + ".part")

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with _connect(remote_dir) as ftp:
                remote = _remote_info(ftp, filename)
                if not force and _is_cached(local_path, remote):
                    logger.info(
                        "%s já está atualizado (versão do FTP de %s UTC); download ignorado",
                        filename,
                        remote.modified.strftime("%d/%m/%Y %H:%M"),
                    )
                    return local_path

                logger.info("Baixando %s (%.1f MB)...", filename, remote.size / 1e6)
                started = time.perf_counter()
                with part_path.open("wb") as fh:
                    ftp.retrbinary(f"RETR {filename}", fh.write)

            received = part_path.stat().st_size
            if received != remote.size:
                raise IncompleteDownloadError(
                    f"{filename}: recebidos {received:,} de {remote.size:,} bytes"
                )
            part_path.replace(local_path)
            timestamp = remote.modified.timestamp()
            os.utime(local_path, (timestamp, timestamp))
            logger.info(
                "Download concluído: %s (%.1f MB em %.1fs)",
                filename,
                remote.size / 1e6,
                time.perf_counter() - started,
            )
            return local_path

        except ftplib.error_perm as exc:
            if str(exc).startswith("550"):
                # Quem chama decide se é mês ainda não publicado ou buraco na série
                logger.info("%s não existe no FTP: %s", filename, str(exc).strip())
                return None
            raise  # erro permanente (login, permissão): repetir não adianta

        except (*ftplib.all_errors, IncompleteDownloadError) as exc:
            if attempt == MAX_ATTEMPTS:
                logger.error("Falha ao baixar %s após %d tentativas: %s", filename, MAX_ATTEMPTS, exc)
                raise
            wait_s = BACKOFF_BASE_S * 2 ** (attempt - 1)
            logger.warning(
                "Falha ao baixar %s (tentativa %d/%d): %s. Nova tentativa em %ds",
                filename,
                attempt,
                MAX_ATTEMPTS,
                exc,
                wait_s,
            )
            time.sleep(wait_s)

        finally:
            part_path.unlink(missing_ok=True)

    raise AssertionError("inalcançável: o laço sempre retorna ou propaga a exceção")


def _source_metadata(dbc_path: Path) -> dict[bytes, bytes]:
    stat = dbc_path.stat()
    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    return {
        b"datasus_source_file": dbc_path.name.encode(),
        b"datasus_source_size": str(stat.st_size).encode(),
        b"datasus_source_modified_utc": modified.isoformat(timespec="seconds").encode(),
    }


def _parquet_is_current(parquet_path: Path, source: dict[bytes, bytes]) -> bool:
    if not parquet_path.exists():
        return False
    try:
        metadata = pq.read_schema(parquet_path).metadata or {}
    except (OSError, pa.ArrowInvalid) as exc:
        logger.warning("%s ilegível, será recriado: %s", parquet_path.name, exc)
        return False
    return all(metadata.get(key) == value for key, value in source.items())


def convert_dbc_to_parquet(dbc_path: Path, dest_dir: Path, force: bool = False) -> Path:
    """Converte .dbc -> .dbf (datasus-dbc) -> .parquet (pyarrow).

    O .dbf intermediário (~9x maior que o .dbc) é apagado ao final. O Parquet
    guarda nos metadados o tamanho e a data do .dbc de origem, que servem para
    pular a conversão quando nada mudou.

    Como o FTP não tem TLS nem hash publicado, a conferência de tamanho do
    download não basta: o read_dbf valida a estrutura do .dbf descompactado
    (registros declarados no cabeçalho x tamanho do arquivo) e o Parquet só
    substitui o anterior se tiver o mesmo número de linhas lidas.

    Raises:
        DBFFormatError: o .dbf descompactado não bate com o próprio cabeçalho.
        ConversionError: o Parquet gravado tem número de linhas diferente do lido.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = dest_dir / f"{dbc_path.stem}.parquet"
    source = _source_metadata(dbc_path)

    if not force and _parquet_is_current(parquet_path, source):
        logger.info("%s já corresponde ao .dbc atual; conversão ignorada", parquet_path.name)
        return parquet_path

    dbf_path = dest_dir / f"{dbc_path.stem}.dbf"
    part_path = parquet_path.with_name(parquet_path.name + ".part")
    logger.info("Convertendo %s para Parquet...", dbc_path.name)
    started = time.perf_counter()
    try:
        datasus_dbc.decompress(str(dbc_path), str(dbf_path))
        df = read_dbf(dbf_path, encoding=DBF_ENCODING)

        table = pa.Table.from_pandas(df, preserve_index=False)
        converted_at = datetime.now(timezone.utc).isoformat(timespec="seconds").encode()
        metadata = {**(table.schema.metadata or {}), **source, b"converted_at_utc": converted_at}
        pq.write_table(table.replace_schema_metadata(metadata), part_path)
        written = pq.read_metadata(part_path).num_rows
        if written != len(df):
            raise ConversionError(f"{parquet_path.name}: gravadas {written:,} de {len(df):,} linhas lidas do .dbf")
        part_path.replace(parquet_path)
    finally:
        dbf_path.unlink(missing_ok=True)
        part_path.unlink(missing_ok=True)

    logger.info(
        "Conversão concluída: %s (%s linhas x %d colunas, %.1f MB) em %.1fs",
        parquet_path.name,
        f"{len(df):,}".replace(",", "."),
        df.shape[1],
        parquet_path.stat().st_size / 1e6,
        time.perf_counter() - started,
    )
    return parquet_path


def _join_filenames(uf: str, months: list[date]) -> str:
    return ", ".join(sih_rd_filename(uf, m) for m in months)


def _report_missing(missing: list[date], found: list[date], uf: str) -> None:
    """Separa meses ainda não publicados (normal) de buracos no meio da série (anormal)."""
    if not missing:
        return
    if not found:
        logger.warning("Nenhuma competência do período está no FTP: %s", _join_filenames(uf, missing))
        return
    last_found = max(found)
    gaps = [m for m in missing if m < last_found]
    pending = [m for m in missing if m > last_found]
    if gaps:
        logger.warning(
            "Faltam no FTP competências anteriores à última publicada (%s); a série terá buraco: %s",
            f"{last_found:%m/%Y}",
            _join_filenames(uf, gaps),
        )
    if pending:
        logger.info(
            "Ainda não publicadas (o DATASUS publica ~5-6 semanas após o fim do mês): %s",
            _join_filenames(uf, pending),
        )


def extract_sih_rd(
    uf: str = DEFAULT_UF,
    start: date | None = None,
    end: date | None = None,
    force: bool = False,
) -> list[Path]:
    """Baixa e converte os arquivos RD de uma UF entre duas competências, inclusive.

    Sem start/end, usa default_period(): janeiro do ano anterior até o mês atual.
    Um arquivo com falha não interrompe os demais; as falhas são reunidas e
    levantadas no final como ExtractionError.

    Returns:
        Os Parquets gerados ou reaproveitados do cache.
    """
    default_start, default_end = default_period()
    months = month_range(start or default_start, end or default_end)
    logger.info(
        "Início da extração SIH-RD %s: competências %s a %s (%d arquivo(s))",
        uf.upper(),
        f"{months[0]:%m/%Y}",
        f"{months[-1]:%m/%Y}",
        len(months),
    )
    started = time.perf_counter()

    parquets: list[Path] = []
    found: list[date] = []
    missing: list[date] = []
    failed: list[str] = []
    for month in months:
        filename = sih_rd_filename(uf, month)
        try:
            dbc_path = download_file(filename, SIH_DATA_DIR, RAW_DIR, force)
            if dbc_path is None:
                missing.append(month)
                continue
            found.append(month)
            parquets.append(convert_dbc_to_parquet(dbc_path, CONVERTED_DIR, force))
        except Exception:
            logger.exception("Falha ao processar %s; seguindo para o próximo arquivo", filename)
            failed.append(filename)

    _report_missing(missing, found, uf)
    logger.info(
        "Fim da extração em %.1fs: %d convertido(s), %d não publicado(s), %d com falha%s",
        time.perf_counter() - started,
        len(parquets),
        len(missing),
        len(failed),
        f" ({', '.join(failed)})" if failed else "",
    )
    if failed:
        raise ExtractionError(f"Falha em {len(failed)} arquivo(s): {', '.join(failed)}")
    return parquets


def _parse_month(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"use o formato AAAA-MM (ex.: 2025-01); recebido {value!r}") from None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baixa e converte a AIH Reduzida (SIH/SUS) do DATASUS.")
    parser.add_argument("--uf", default=DEFAULT_UF, help=f"sigla da UF (padrão: {DEFAULT_UF})")
    parser.add_argument(
        "--inicio", type=_parse_month, metavar="AAAA-MM", help="primeira competência (padrão: janeiro do ano anterior)"
    )
    parser.add_argument("--fim", type=_parse_month, metavar="AAAA-MM", help="última competência (padrão: mês atual)")
    parser.add_argument("--forcar", action="store_true", help="baixa e converte de novo, ignorando o cache")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    setup_logging()
    try:
        extract_sih_rd(args.uf, args.inicio, args.fim, args.forcar)
    except (ExtractionError, ValueError) as exc:
        logger.error("Extração terminou com erro: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
