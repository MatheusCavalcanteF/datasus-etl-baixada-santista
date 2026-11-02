"""Testes do .dbf descompactado: validação estrutural, conversão do extract e minimização dos campos pessoais."""

from __future__ import annotations

import logging
import struct
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
from dbfread import DBF
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from src import extract
from src.dbf_reader import DBFFormatError, read_dbf, read_dbf_bytes

# Dois campos no layout do RD: N_AIH (texto, 13) e DIAS_PERM (numérico, 4)
FIELDS = (("N_AIH", "C", 13, 0), ("DIAS_PERM", "N", 4, 0))
LDID_CP850 = 0x02  # code page que os RD do SIH declaram no cabeçalho


def build_dbf(
    rows: list[tuple],
    deleted: frozenset[int] = frozenset(),
    declared: int | None = None,
    fields: tuple[tuple[str | bytes, str, int, int], ...] = FIELDS,
    ldid: int = LDID_CP850,
    terminator: bytes = b"\x0d",
) -> bytes:
    """Monta um dBase III mínimo; `declared` permite mentir no número de registros do cabeçalho.

    Valores e nomes bytes entram crus; os demais são codificados em cp850, como nos RD.
    `terminator` troca o 0x0D do fim do cabeçalho (os ST do CNES trazem 0x00).
    """
    record_len = 1 + sum(length for _, _, length, _ in fields)
    header_len = 32 + 32 * len(fields) + 1
    n_declared = len(rows) if declared is None else declared
    header = struct.pack("<B3sIHH17xB2x", 0x03, b"\x7e\x01\x01", n_declared, header_len, record_len, ldid)
    for name, kind, length, decimals in fields:
        raw_name = name if isinstance(name, bytes) else name.encode()
        header += struct.pack("<11sc4xBB14x", raw_name, kind.encode(), length, decimals)
    header += terminator

    def encode(value: object, kind: str, length: int) -> bytes:
        text = value if isinstance(value, bytes) else str(value).encode("cp850")
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

    def test_leitura_em_memoria_igual_a_do_arquivo(self, dbf_path):
        raw = build_dbf(ROWS, deleted=frozenset({1}))
        dbf_path.write_bytes(raw)
        assert read_dbf_bytes(raw, dbf_path.name).equals(read_dbf(dbf_path))


# Texto real de AUD_JUST nos RD: "ç" é 0x87 em cp850 e caractere de controle em latin-1
AUD_JUST = (("AUD_JUST", "C", 20, 0),)


class TestEncoding:
    def test_usa_a_code_page_do_cabecalho(self, dbf_path):
        dbf_path.write_bytes(build_dbf([("INFORMAÇÃO úé",)], fields=AUD_JUST))
        assert read_dbf(dbf_path)["AUD_JUST"].tolist() == ["INFORMAÇÃO úé"]

    def test_cp1252_declarado_no_cabecalho(self, dbf_path):
        dbf_path.write_bytes(build_dbf([("AÇÃO".encode("cp1252"),)], fields=AUD_JUST, ldid=0x03))
        assert read_dbf(dbf_path)["AUD_JUST"].tolist() == ["AÇÃO"]

    def test_sem_code_page_le_latin1_e_registra(self, dbf_path, caplog):
        # Como nos ST do CNES: LDID 0 e "Nº" gravado como 0xBA (latin-1)
        caplog.set_level(logging.INFO)
        dbf_path.write_bytes(build_dbf([("Nº 167".encode("latin-1"),)], fields=AUD_JUST, ldid=0x00))
        assert read_dbf(dbf_path)["AUD_JUST"].tolist() == ["Nº 167"]
        assert "sem code page conhecida (LDID 0x00)" in caplog.text

    def test_byte_indefinido_na_code_page_nao_se_perde(self, dbf_path):
        # Como no AP_CNSPCN do AD do SIA: LDID 0x57 (cp1252) e o byte 0x81, que o cp1252 não define
        raw = b"\x82{\x81~|\x84"
        fields = (("AP_CNSPCN", "C", 6, 0),)
        dbf_path.write_bytes(build_dbf([(raw,)], fields=fields, ldid=0x57))
        value = read_dbf(dbf_path)["AP_CNSPCN"].tolist()[0]
        assert value == "‚{\x81~|„"  # 0x82 e 0x84 existem no cp1252 (‚ „); 0x81 vira U+0081
        # Todo byte original pode ser recuperado
        assert bytes(0x81 if ch == "\x81" else ch.encode("cp1252")[0] for ch in value) == raw

    def test_ldid_0x58_dos_st_antigos_e_cp1252(self, dbf_path):
        dbf_path.write_bytes(build_dbf([("AÇÃO".encode("cp1252"),)], fields=AUD_JUST, ldid=0x58))
        assert read_dbf(dbf_path)["AUD_JUST"].tolist() == ["AÇÃO"]

    def test_encoding_explicito_prevalece_sobre_o_cabecalho(self, dbf_path):
        dbf_path.write_bytes(build_dbf([("AÇÃO".encode("latin-1"),)], fields=AUD_JUST))
        assert read_dbf(dbf_path, encoding="latin-1")["AUD_JUST"].tolist() == ["AÇÃO"]


class TestCasosDeBorda:
    def test_cabecalho_sem_terminador_como_nos_st_do_cnes(self, dbf_path):
        # O dbfread lê os registros como se fossem descritores de campo e quebra
        dbf_path.write_bytes(build_dbf(ROWS, terminator=b"\x00"))
        assert read_dbf(dbf_path)["DIAS_PERM"].tolist() == [4, 0, 12]

    def test_inteiro_grande_com_vazio_nao_perde_precisao(self, dbf_path):
        fields = (("X", "N", 18, 0),)
        dbf_path.write_bytes(build_dbf([("123456789012345678",), ("",)], fields=fields))
        assert read_dbf(dbf_path)["X"].tolist() == [123456789012345678, pd.NA]

    def test_fracao_em_campo_sem_decimais_vira_float_com_aviso(self, dbf_path, caplog):
        fields = (("X", "N", 4, 0),)
        dbf_path.write_bytes(build_dbf([("1.5",), ("2",)], fields=fields))
        assert read_dbf(dbf_path)["X"].tolist() == [1.5, 2.0]
        assert "1 valor(es) fracionário(s)" in caplog.text

    def test_numero_invalido_vira_nulo_com_aviso(self, dbf_path, caplog):
        fields = (("X", "N", 5, 2),)
        dbf_path.write_bytes(build_dbf([("1,50",), ("*****",), ("2.25",)], fields=fields))
        df = read_dbf(dbf_path)
        assert df["X"].isna().tolist() == [True, True, False]
        assert "2 valor(es) não numérico(s)" in caplog.text

    def test_data_invalida_vira_nulo_com_aviso_e_zerada_sem_aviso(self, dbf_path, caplog):
        fields = (("X", "D", 8, 0),)
        dbf_path.write_bytes(build_dbf([("20250131",), ("20250230",), ("00000000",), ("",)], fields=fields))
        df = read_dbf(dbf_path)
        assert df["X"].tolist()[0] == pd.Timestamp("2025-01-31")
        assert df["X"].isna().tolist() == [False, True, True, True]
        assert "1 valor(es) de data inválida(s)" in caplog.text

    def test_logico_vira_booleano_anulavel(self, dbf_path):
        fields = (("X", "L", 1, 0),)
        dbf_path.write_bytes(build_dbf([("T",), ("f",), ("?",), ("",)], fields=fields))
        df = read_dbf(dbf_path)
        assert str(df["X"].dtype) == "boolean"
        assert df["X"].tolist() == [True, False, pd.NA, pd.NA]

    def test_tipo_sem_suporte_e_rejeitado_so_quando_pedido(self, dbf_path):
        fields = (("N_AIH", "C", 13, 0), ("ID", "I", 4, 0))
        dbf_path.write_bytes(build_dbf([("3525100000001", struct.pack("<i", 42))], fields=fields))
        with pytest.raises(DBFFormatError, match=r"sem suporte: ID \(I\)"):
            read_dbf(dbf_path)
        assert read_dbf(dbf_path, columns=["N_AIH"])["N_AIH"].tolist() == ["3525100000001"]

    def test_nome_de_campo_nao_ascii_e_rejeitado(self, dbf_path):
        dbf_path.write_bytes(build_dbf([("ab",)], fields=((b"A\xc7AO", "C", 2, 0),)))
        with pytest.raises(DBFFormatError, match="não é ASCII"):
            read_dbf(dbf_path)


# Texto ASCII sem espaço nas pontas: o dbfread só tira os espaços da direita
_texto = st.text(st.characters(min_codepoint=33, max_codepoint=126), max_size=10)
_inteiro = st.one_of(st.none(), st.integers(min_value=-(10**8) + 1, max_value=10**9 - 1))


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(rows=st.lists(st.tuples(_texto, _inteiro), max_size=30), deleted=st.sets(st.integers(0, 29)))
def test_mesma_saida_que_o_dbfread(tmp_path, rows, deleted):
    fields = (("TXT", "C", 10, 0), ("NUM", "N", 9, 0))
    path = tmp_path / "prop.dbf"
    path.write_bytes(build_dbf([(t, "" if n is None else n) for t, n in rows], frozenset(deleted), fields=fields))

    ours = read_dbf(path)
    reference = list(DBF(path, encoding="cp850"))
    assert len(ours) == len(reference)
    assert ours["TXT"].tolist() == [r["TXT"] for r in reference]
    assert [None if pd.isna(v) else v for v in ours["NUM"]] == [r["NUM"] for r in reference]


class TestConversaoDoExtract:
    @pytest.fixture
    def fake_decompress(self, monkeypatch):
        """Substitui o datasus-dbc: o "descompactado" é o conteúdo definido pelo teste."""
        content: dict[str, bytes] = {}
        monkeypatch.setattr(extract.datasus_dbc, "decompress_bytes", lambda dbc: content["dbf"])
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

    def test_parquet_de_versao_anterior_e_reconvertido_sem_force(self, tmp_path, fake_decompress, monkeypatch):
        # Os Parquets da versão 1 têm texto decodificado como latin-1 e precisam sair do cache
        fake_decompress["dbf"] = build_dbf([("INFORMAÇÃO",)], fields=AUD_JUST)
        dbc = tmp_path / "RDSP9901.dbc"
        dbc.write_bytes(b"dbc")
        converted = tmp_path / "converted"
        with monkeypatch.context() as m:
            m.setattr(extract, "CONVERSION_VERSION", "1")
            extract.convert_dbc_to_parquet(dbc, converted)

        novo = extract.convert_dbc_to_parquet(dbc, converted)
        table = pq.read_table(novo)
        assert table.schema.metadata[b"conversion_version"] == extract.CONVERSION_VERSION.encode()
        assert table.column("AUD_JUST").to_pylist() == ["INFORMAÇÃO"]


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
        monkeypatch.setattr(extract.datasus_dbc, "decompress_bytes", lambda dbc: dbf)
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
