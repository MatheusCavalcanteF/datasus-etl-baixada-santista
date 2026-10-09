"""Testes do transform com AIHs sintéticas no layout do DATASUS.

Cobrem os três pontos em que um erro silencioso distorceria o painel: a chave
das AIHs de longa permanência, o recorte geográfico e os dias de permanência
que alimentam a média de permanência.
"""

import pytest

from src import transform
from src.config import BAIXADA_SANTISTA

# Números de AIH reais, dos casos que motivaram a chave atual
AIH_DUAS_FATIAS_NA_COMPETENCIA = "3525115496969"  # competência 05/2025
AIH_NORMAL_E_PRORROGACAO = "3525133320698"  # competência 01/2026

FORA_DA_BAIXADA = [
    "355030",  # São Paulo
    "354870",  # São Bernardo do Campo
    "330455",  # Rio de Janeiro
    "3548500",  # Santos com dígito verificador: o SIH usa 6 dígitos
    "35485",  # código truncado
    "",
]


class TestChaveUnica:
    def test_fatias_de_longa_permanencia_na_mesma_competencia_sao_mantidas(self, make_row, run_transform):
        """O hospital apresentou a fatia de abril, atrasada, junto com a de maio."""
        aih = AIH_DUAS_FATIAS_NA_COMPETENCIA
        rows = [
            make_row(N_AIH=aih, IDENT="1", MES_CMPT="03", DT_INTER="20250301", DT_SAIDA="20250331",
                     DIAS_PERM=30, VAL_TOT=4000.0, COBRANCA="21"),
            make_row(N_AIH=aih, IDENT="5", MES_CMPT="05", DT_INTER="20250301", DT_SAIDA="20250430",
                     DIAS_PERM=30, VAL_TOT=4273.80, COBRANCA="21"),
            make_row(N_AIH=aih, IDENT="5", MES_CMPT="05", DT_INTER="20250301", DT_SAIDA="20250527",
                     DIAS_PERM=27, VAL_TOT=2143.96, COBRANCA="12"),
        ]

        df = run_transform(rows)

        assert len(df) == 3
        assert not df.duplicated(transform.KEY_COLUMNS).any()
        # Sem data_saida na chave, as duas fatias de maio colidiriam
        assert df.duplicated(["id_aih", "competencia", "tipo_aih"]).sum() == 1
        assert df["dias_permanencia"].sum() == 87
        assert df["valor_total"].sum() == pytest.approx(10_417.76)
        assert (df["tipo_aih"] == transform.TIPO_AIH_NORMAL).sum() == 1  # uma única internação

    def test_aih_normal_e_prorrogacao_na_mesma_competencia_sao_mantidas(self, make_row, run_transform):
        aih = AIH_NORMAL_E_PRORROGACAO
        rows = [
            make_row(N_AIH=aih, IDENT="1", ANO_CMPT="2026", MES_CMPT="01", DT_INTER="20251219",
                     DT_SAIDA="20251231", DIAS_PERM=12, COBRANCA="28"),
            make_row(N_AIH=aih, IDENT="5", ANO_CMPT="2026", MES_CMPT="01", DT_INTER="20251219",
                     DT_SAIDA="20260106", DIAS_PERM=6, COBRANCA="12"),
        ]

        df = run_transform(rows)

        assert len(df) == 2
        assert set(df["tipo_aih"]) == {"Normal", "Longa permanência"}
        assert set(df["desfecho"]) == {"Permanência", "Alta"}

    def test_copia_exata_bloqueia_a_gravacao(self, make_row, run_transform, trusted_path):
        row = make_row()

        with pytest.raises(transform.TrustedValidationError, match="repetida"):
            run_transform([row, dict(row)])

        assert not trusted_path.exists()


class TestFiltroGeografico:
    def test_mantem_somente_hospitais_da_baixada_santista(self, make_row, run_transform):
        dentro = [make_row(MUNIC_MOV=code) for code in BAIXADA_SANTISTA]
        fora = [make_row(MUNIC_MOV=code) for code in FORA_DA_BAIXADA]

        df = run_transform(dentro + fora)

        assert len(df) == len(BAIXADA_SANTISTA)
        assert set(df["id_municipio"]) == set(BAIXADA_SANTISTA)

    def test_filtro_e_pelo_municipio_do_hospital_nao_da_residencia(self, make_row, run_transform):
        paulistano_internado_em_santos = make_row(MUNIC_MOV="354850", MUNIC_RES="355030")
        santista_internado_em_sao_paulo = make_row(MUNIC_MOV="355030", MUNIC_RES="354850")

        df = run_transform([paulistano_internado_em_santos, santista_internado_em_sao_paulo])

        assert df["id_aih"].tolist() == [paulistano_internado_em_santos["N_AIH"]]
        assert df.loc[0, "id_municipio_residencia"] == "355030"

    def test_validacao_barra_municipio_de_fora_que_escape_do_filtro(self, make_row, run_transform):
        df = run_transform([make_row()])
        df.loc[0, "id_municipio"] = "355030"

        with pytest.raises(transform.TrustedValidationError, match="fora da Baixada"):
            transform.validate_trusted(df)


class TestDiasDePermanencia:
    def test_dias_nulo_em_aih_normal_e_recalculado_pelas_datas(self, make_row, run_transform):
        df = run_transform([make_row(DT_INTER="20250110", DT_SAIDA="20250116", DIAS_PERM=None)])

        assert df.loc[0, "dias_permanencia"] == 6

    def test_alta_no_mesmo_dia_tem_zero_dias_e_e_mantida(self, make_row, run_transform):
        df = run_transform([make_row(DT_INTER="20250110", DT_SAIDA="20250110", DIAS_PERM=0)])

        assert df.loc[0, "dias_permanencia"] == 0

    @pytest.mark.parametrize(
        ("dt_inter", "dt_saida"),
        [
            pytest.param("20251340", "20251341", id="mes-13"),
            pytest.param("", "20250105", id="entrada-vazia"),
            pytest.param("20250105", "", id="saida-vazia"),
            pytest.param("20250110", "20250105", id="saida-antes-da-entrada"),
        ],
    )
    def test_datas_invalidas_descartam_a_aih(self, make_row, run_transform, dt_inter, dt_saida):
        valida = make_row()
        invalida = make_row(DT_INTER=dt_inter, DT_SAIDA=dt_saida)

        df = run_transform([valida, invalida])

        assert df["id_aih"].tolist() == [valida["N_AIH"]]

    def test_longa_permanencia_sem_dias_e_descartada(self, make_row, run_transform):
        """Na prorrogação, DT_INTER é a entrada original, então as datas não dão os dias daquela fatia."""
        valida = make_row()
        fatia = make_row(IDENT="5", DT_INTER="20250101", DT_SAIDA="20250228", DIAS_PERM=None)

        df = run_transform([valida, fatia])

        assert df["id_aih"].tolist() == [valida["N_AIH"]]

    def test_dias_negativo_descarta_a_aih(self, make_row, run_transform):
        valida = make_row()
        negativa = make_row(DIAS_PERM=-3)

        df = run_transform([valida, negativa])

        assert df["id_aih"].tolist() == [valida["N_AIH"]]
        assert df["dias_permanencia"].dtype == "int64"
