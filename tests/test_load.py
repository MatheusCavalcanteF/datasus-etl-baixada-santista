"""Testes do load: média de permanência no banco, idempotência e atomicidade da carga."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from src import load

# Mesma fórmula da medida DAX "Média de Permanência (ALOS)": soma dos dias ÷ internações
ALOS_SQL = "SELECT 1.0 * SUM(dias_permanencia) / SUM(tipo_aih = 'Normal') FROM Fato_Internacao"
AIH_LONGA = "3525115496969"


def _query(db: Path, sql: str) -> tuple:
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(sql).fetchone()


def _snapshot(db: Path) -> tuple:
    """Assinatura do banco: contagens de todas as tabelas, soma de valores e lista de AIHs."""
    counts = tuple(_query(db, f"SELECT COUNT(*) FROM {table}")[0] for table in load.SCHEMA)
    total = _query(db, "SELECT SUM(valor_total), GROUP_CONCAT(id_aih) FROM Fato_Internacao")
    return counts + total


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "hospital.db"


class TestMediaDePermanenciaNoBanco:
    def test_cada_internacao_conta_uma_vez_e_os_dias_de_todas_as_fatias_somam(
        self, make_row, run_transform, trusted_path, db_path
    ):
        rows = [
            make_row(DT_INTER="20250110", DT_SAIDA="20250110", DIAS_PERM=0),  # alta no mesmo dia
            make_row(DT_INTER="20250110", DT_SAIDA="20250114", DIAS_PERM=4),
            make_row(DT_INTER="20250110", DT_SAIDA="20250116", DIAS_PERM=None),  # recalculado: 6
            # Uma internação longa em três AIHs: 30 + 30 + 27 = 87 dias
            make_row(N_AIH=AIH_LONGA, IDENT="1", MES_CMPT="03", DT_INTER="20250301", DT_SAIDA="20250331",
                     DIAS_PERM=30, COBRANCA="21"),
            make_row(N_AIH=AIH_LONGA, IDENT="5", MES_CMPT="05", DT_INTER="20250301", DT_SAIDA="20250430",
                     DIAS_PERM=30, COBRANCA="21"),
            make_row(N_AIH=AIH_LONGA, IDENT="5", MES_CMPT="05", DT_INTER="20250301", DT_SAIDA="20250527",
                     DIAS_PERM=27, COBRANCA="12"),
            make_row(DT_INTER="20250120", DT_SAIDA="20250115", DIAS_PERM=5),  # saída antes da entrada: sai
        ]
        run_transform(rows)
        load.load(trusted_path, db_path)

        dias, internacoes, linhas = _query(
            db_path, "SELECT SUM(dias_permanencia), SUM(tipo_aih = 'Normal'), COUNT(*) FROM Fato_Internacao"
        )
        assert (dias, internacoes, linhas) == (97, 4, 6)
        assert _query(db_path, ALOS_SQL)[0] == pytest.approx(97 / 4)  # 24,25 dias
        # A média simples por linha trataria as 3 fatias como 3 internações e daria 16,2 dias
        assert _query(db_path, "SELECT AVG(dias_permanencia) FROM Fato_Internacao")[0] == pytest.approx(97 / 6)

    def test_recorte_sem_internacao_devolve_nulo_em_vez_de_falhar(
        self, make_row, run_transform, trusted_path, db_path
    ):
        """Equivale a filtrar só 'Longa permanência' no Power BI, onde o DIVIDE devolve BLANK."""
        run_transform([make_row(N_AIH=AIH_LONGA, IDENT="5", DIAS_PERM=30)])
        load.load(trusted_path, db_path)

        assert _query(db_path, ALOS_SQL)[0] is None


class TestCargaAtomica:
    def test_carga_repetida_produz_o_mesmo_banco(self, make_row, run_transform, trusted_path, db_path):
        run_transform([make_row() for _ in range(3)])

        primeira = load.load(trusted_path, db_path)
        assinatura = _snapshot(db_path)
        segunda = load.load(trusted_path, db_path)

        assert primeira == segunda
        assert _snapshot(db_path) == assinatura
        assert _query(db_path, "SELECT COUNT(*) FROM Fato_Internacao")[0] == 3

    @pytest.mark.parametrize("falha", ["fk_orfa", "validacao"])
    def test_falha_no_meio_da_carga_mantem_o_banco_anterior(
        self, make_row, run_transform, trusted_path, db_path, monkeypatch, falha
    ):
        run_transform([make_row() for _ in range(3)])
        load.load(trusted_path, db_path)
        antes = _snapshot(db_path)

        run_transform([make_row()])  # a nova carga substituiria as 3 AIHs por 1
        if falha == "fk_orfa":
            original = load.build_fato_internacao

            def fato_com_cid_inexistente(df):
                fato = original(df)
                fato["id_cid"] = "ZZZZ"
                return fato

            monkeypatch.setattr(load, "build_fato_internacao", fato_com_cid_inexistente)
            erro_esperado = sqlite3.IntegrityError
        else:

            def validacao_que_falha(conn, trusted):
                raise load.LoadValidationError("falha simulada")

            monkeypatch.setattr(load, "validate_load", validacao_que_falha)
            erro_esperado = load.LoadValidationError

        with pytest.raises(erro_esperado):
            load.load(trusted_path, db_path)

        assert _snapshot(db_path) == antes
