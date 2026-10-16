"""Testes do .dbf descompactado: validação estrutural, conversão do extract e minimização dos campos pessoais."""

from __future__ import annotations

import struct
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src import extract
from src.dbf_reader import DBFFormatError, read_dbf

# Dois campos no layout do RD: N_AIH (texto, 13) e DIAS_PERM (numérico, 4)
FIELDS = (("N_AIH", "C", 13, 0), ("DIAS_PERM", "N", 4, 0))


def build_dbf(
    rows: list[tuple],
    deleted: frozenset[int] = frozenset(),
    declared: int | None = None,
    fields: tuple[tuple[str, str, int, int], ...] = FIELDS,
) -> bytes:
    """Monta um dBase III mínimo; `declared` permite mentir no número de registros do cabeçalho."""
    record_len = 1 + sum(length for _, _, length, _ in fields)
    header_len = 32 + 32 * len(fields) + 1
    n_declared = len(rows) if declared is None else declared
    header = struct.pack("<B3sIHH20x", 0x03, b"\x7e\x01\x01", n_declared, header_len, record_len)
    for name, kind, length, decimals in fields:
        header += struct.pack("<11sc4xBB14x", name.encode(), kind.encode(), length, decimals)
    header += b"\x0d"

    def encode(value: object, kind: str, length: int) -> bytes:
        text = str(value).encode()
        return text.rjust(length) if kind == "N" else text.ljust(length)

    body = b"".join(
        (b"*" if i in deleted else b" ")
        + b"".join(encode(value, kind, length) for value, (_, kind, length, _) in zip(row, fields))
        for i, row in enumerate(rows)
    )
    return header + body


ROWS = [("3525100000001", 4), ("3525100000002", 0), ("3525100000003", 12)]


@pytest.fixture
def dbf_path(tmp_path: Path) -> Path:
    return tmp_path / "RDSP9901.dbf"


class TestEstruturaDoDbf:
    def test_le_exatamente_os_registros_declarados_no_cabecalho(self, dbf_path):
        dbf_path.write_bytes(build_dbf(ROWS))
        df = read_dbf(dbf_path)
        assert df["N_AIH"].tolist() == [aih for aih, _ in ROWS]
        assert df["DIAS_PERM"].tolist() == [4, 0, 12]

    def test_aceita_o_byte_0x1a_de_fim_de_arquivo(self, dbf_path):
        dbf_path.write_bytes(build_dbf(ROWS) + b"\x1a")
        assert len(read_dbf(dbf_path)) == len(ROWS)

    def test_registros_excluidos_saem_da_leitura(self, dbf_path):
        dbf_path.write_bytes(build_dbf(ROWS, deleted=frozenset({1})))
        assert read_dbf(dbf_path)["N_AIH"].tolist() == ["3525100000001", "3525100000003"]

    def test_arquivo_truncado_e_rejeitado(self, dbf_path):
        dbf_path.write_bytes(build_dbf(ROWS)[:-5])
        with pytest.raises(DBFFormatError, match="truncado"):
            read_dbf(dbf_path)

    def test_cabecalho_com_menos_registros_que_o_arquivo_e_rejeitado(self, dbf_path):
        # Sem a validação, o terceiro registro sumiria sem aviso
        dbf_path.write_bytes(build_dbf(ROWS, declared=2))
        with pytest.raises(DBFFormatError, match="além dos 2 registros declarados"):
            read_dbf(dbf_path)

    def test_lixo_depois_dos_registros_e_rejeitado(self, dbf_path):
        dbf_path.write_bytes(build_dbf(ROWS) + b"\x1a\x00")
        with pytest.raises(DBFFormatError, match="2 byte"):
            read_dbf(dbf_path)

    def test_registro_desalinhado_e_rejeitado(self, dbf_path):
        raw = bytearray(build_dbf(ROWS))
        header_len = struct.unpack("<H", raw[8:10])[0]
        raw[header_len + 18] = ord("X")  # marca de exclusão do 2º registro (registro de 18 bytes)
        dbf_path.write_bytes(bytes(raw))
        with pytest.raises(DBFFormatError, match="1 registro"):
            read_dbf(dbf_path)


class TestConversaoDoExtract:
    @pytest.fixture
    def fake_decompress(self, monkeypatch):
        """Substitui o datasus-dbc: o "descompactado" é o conteúdo definido pelo teste."""
        content: dict[str, bytes] = {}

        def _decompress(src: str, dest: str) -> None:
            Path(dest).write_bytes(content["dbf"])

        monkeypatch.setattr(extract.datasus_dbc, "decompress", _decompress)
        return content

    def test_dbf_valido_vira_parquet_com_as_mesmas_linhas(self, tmp_path, fake_decompress):
        fake_decompress["dbf"] = build_dbf(ROWS)
        dbc = tmp_path / "RDSP9901.dbc"
        dbc.write_bytes(b"dbc")
        parquet = extract.convert_dbc_to_parquet(dbc, tmp_path / "converted")
        assert pq.read_metadata(parquet).num_rows == len(ROWS)
        assert not (tmp_path / "converted" / "RDSP9901.dbf").exists()

    def test_dbf_inconsistente_nao_substitui_o_parquet_anterior(self, tmp_path, fake_decompress):
        dbc = tmp_path / "RDSP9901.dbc"
        dbc.write_bytes(b"dbc")
        converted = tmp_path / "converted"
        fake_decompress["dbf"] = build_dbf(ROWS)
        parquet = extract.convert_dbc_to_parquet(dbc, converted)
        before = parquet.read_bytes()

        fake_decompress["dbf"] = build_dbf(ROWS, declared=2)
        with pytest.raises(DBFFormatError):
            extract.convert_dbc_to_parquet(dbc, converted, force=True)

        assert parquet.read_bytes() == before
        assert sorted(p.name for p in converted.iterdir()) == ["RDSP9901.parquet"]


# Layout com os campos pessoais que o RD publica, nas posições reais (CEP e NASC antes de SEXO)
FIELDS_PESSOAIS = (
    ("N_AIH", "C", 13, 0),
    ("CEP", "C", 8, 0),
    ("NASC", "D", 8, 0),
    ("SEXO", "C", 1, 0),
    ("GESTOR_CPF", "C", 15, 0),
    ("DIAS_PERM", "N", 4, 0),
)
ROWS_PESSOAIS = [
    ("3525100000001", "11010000", "19900411", "1", "000001084165856", 4),
    ("3525100000002", "11320000", "20250929", "3", "000000000000000", 0),
]


class TestMinimizacaoDeDadosPessoais:
    @pytest.fixture
    def dbc(self, tmp_path, monkeypatch) -> Path:
        dbf = build_dbf(ROWS_PESSOAIS, fields=FIELDS_PESSOAIS)
        monkeypatch.setattr(extract.datasus_dbc, "decompress", lambda src, dest: Path(dest).write_bytes(dbf))
        path = tmp_path / "RDSP9901.dbc"
        path.write_bytes(b"dbc")
        return path

    def test_cep_nascimento_e_cpf_do_gestor_nao_chegam_ao_converted(self, tmp_path, dbc):
        parquet = extract.convert_dbc_to_parquet(dbc, tmp_path / "converted")
        table = pq.read_table(parquet)
        assert table.column_names == ["N_AIH", "SEXO", "DIAS_PERM"]
        assert table.num_rows == len(ROWS_PESSOAIS)
        assert table.schema.metadata[b"dropped_personal_columns"] == b"CEP,NASC,GESTOR_CPF"
        assert table.column("N_AIH").to_pylist() == [aih for aih, *_ in ROWS_PESSOAIS]

    def test_parquet_gravado_antes_da_minimizacao_e_reconvertido_sem_force(self, tmp_path, dbc, monkeypatch):
        converted = tmp_path / "converted"
        with monkeypatch.context() as m:
            m.setattr(extract, "PERSONAL_DATA_COLUMNS", ())  # comportamento antigo: todas as colunas
            antigo = extract.convert_dbc_to_parquet(dbc, converted)
        assert "CEP" in pq.read_schema(antigo).names

        novo = extract.convert_dbc_to_parquet(dbc, converted)
        assert novo == antigo
        assert not {"CEP", "NASC", "GESTOR_CPF"} & set(pq.read_schema(novo).names)
