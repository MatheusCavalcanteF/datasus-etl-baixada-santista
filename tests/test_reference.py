"""Testes dos dados de referência (API do CNES e tabela CID-10) e do uso deles no load, sem rede."""

from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import urllib.error
import zipfile
from contextlib import closing
from pathlib import Path

import pandas as pd
import pytest

from src import load, reference

# Recorte da tabela do DATASUS, com o mesmo formato: latin-1, ";" e ";" no fim da linha
SUBCATEGORIAS = (
    "SUBCAT;CLASSIF;RESTRSEXO;CAUSAOBITO;DESCRICAO;DESCRABREV;REFER;EXCLUIDOS;\n"
    "J189;;;;Pneumonia não especificada;J18.9 Pneumonia NE;;;\n"
    "N180;;;;Doença renal em estádio final;N18.0 Doenca renal estadio final;;;\n"
)
CATEGORIAS = (
    "CAT;CLASSIF;DESCRICAO;DESCRABREV;REFER;EXCLUIDOS;\n"
    "J18;;Pneumonia por microorganismo não especificado;J18 Pneumonia;;;\n"
    "N18;;Insuficiência renal crônica;N18 Insuf renal cronica;;;\n"
    "O80;;Parto único espontâneo;O80 Parto unico espontaneo;;;\n"
)
CAPITULOS = (
    "NUMCAP;CATINIC;CATFIM;DESCRICAO;DESCRABREV;\n"
    "10;J00;J99;Capítulo X - Doenças do aparelho respiratório;X. Doencas ap respiratorio;\n"
    "14;N00;N99;Capítulo XIV - Doenças do aparelho geniturinário;XIV. Doencas ap geniturinario;\n"
    "15;O00;O99;Capítulo XV - Gravidez, parto e puerpério;XV. Gravidez parto puerperio;\n"
)


def cid10_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CID-10-SUBCATEGORIAS.CSV", SUBCATEGORIAS.encode("latin-1"))
        archive.writestr("CID-10-CATEGORIAS.CSV", CATEGORIAS.encode("latin-1"))
        archive.writestr("CID-10-CAPITULOS.CSV", CAPITULOS.encode("latin-1"))
    return buffer.getvalue()


def cnes_json(cnes: str, fantasia: str | None = "SANTA CASA DE SANTOS", municipio: int = 354850) -> bytes:
    return json.dumps({
        "codigo_cnes": int(cnes),
        "nome_fantasia": fantasia,
        "nome_razao_social": "IRMANDADE DA SANTA CASA DA MISERICORDIA DE SANTOS",
        "codigo_municipio": municipio,
    }).encode()


@pytest.fixture
def fake_http(monkeypatch):
    """Substitui o http_get: responde pelo dicionário de URLs e registra as chamadas."""
    responses: dict[str, bytes | Exception] = {}
    calls: list[str] = []

    def _get(url: str) -> bytes:
        calls.append(url)
        response = responses.get(url, reference.NotFoundError(url))
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(reference, "http_get", _get)
    return responses, calls


def cnes_url(cnes: str) -> str:
    return reference.CNES_API_URL.format(cnes=cnes)


class TestTabelaCid10:
    def test_subcategoria_e_categoria_com_capitulo(self):
        cid10 = reference.parse_cid10_zip(cid10_zip()).set_index("id_cid")
        assert cid10.loc["J189", "descricao_cid"] == "Pneumonia não especificada"
        assert cid10.loc["J189", "descricao_categoria"] == "Pneumonia por microorganismo não especificado"
        assert cid10.loc["J189", "numero_capitulo"] == 10
        assert cid10.loc["J189", "capitulo"] == "Capítulo X - Doenças do aparelho respiratório"
        assert cid10.loc["O80", "descricao_cid"] == "Parto único espontâneo"  # categoria sem subcategoria

    def test_arquivo_com_hash_diferente_e_recusado(self, tmp_path, fake_http):
        responses, _ = fake_http
        responses[reference.CID10_URL] = cid10_zip()  # o recorte não é o arquivo oficial
        with pytest.raises(reference.ReferenceDataError, match="SHA-256 inesperado"):
            reference.update_cid10(tmp_path / "cid10.parquet")
        assert not (tmp_path / "cid10.parquet").exists()

    def test_nao_baixa_de_novo_sem_forcar(self, tmp_path, fake_http, monkeypatch):
        responses, calls = fake_http
        data = cid10_zip()
        responses[reference.CID10_URL] = data
        monkeypatch.setattr(reference, "CID10_SHA256", hashlib.sha256(data).hexdigest())
        path = tmp_path / "cid10.parquet"

        reference.update_cid10(path)
        reference.update_cid10(path)
        assert calls == [reference.CID10_URL]


class TestNomesDosHospitais:
    def test_usa_o_nome_fantasia(self, fake_http):
        responses, _ = fake_http
        responses[cnes_url("2025752")] = cnes_json("2025752")
        hospital = reference.fetch_hospital("2025752")
        assert hospital["nome_hospital"] == "SANTA CASA DE SANTOS"
        assert hospital["id_municipio_cnes"] == "354850"

    def test_sem_nome_fantasia_usa_a_razao_social(self, fake_http):
        responses, _ = fake_http
        responses[cnes_url("2025752")] = cnes_json("2025752", fantasia="  ")
        assert reference.fetch_hospital("2025752")["nome_hospital"].startswith("IRMANDADE DA SANTA CASA")

    def test_resposta_de_outro_cnes_e_recusada(self, fake_http):
        responses, _ = fake_http
        responses[cnes_url("2025752")] = cnes_json("2079720")
        with pytest.raises(reference.ReferenceDataError, match="outro estabelecimento"):
            reference.fetch_hospital("2025752")

    def test_consulta_so_o_que_falta_e_grava_o_que_deu_certo(self, tmp_path, fake_http):
        responses, calls = fake_http
        path = tmp_path / "cnes.parquet"
        responses[cnes_url("2025752")] = cnes_json("2025752")
        reference.update_hospitals(["2025752"], path)

        responses[cnes_url("2079720")] = cnes_json("2079720", fantasia="HOSPITAL GUILHERME ALVARO SANTOS")
        responses[cnes_url("9999999")] = urllib.error.URLError("rede fora")
        with pytest.raises(reference.ReferenceDataError, match="9999999"):
            reference.update_hospitals(["2025752", "2079720", "9999999"], path)

        assert calls.count(cnes_url("2025752")) == 1  # já estava no cache
        assert reference.read_hospitals(path)["id_hospital"].tolist() == ["2025752", "2079720"]

    def test_cnes_inexistente_na_api_nao_e_erro(self, tmp_path, fake_http):
        cache = reference.update_hospitals(["1234567"], tmp_path / "cnes.parquet")
        assert cache.empty


class TestHttpGet:
    def test_tenta_de_novo_em_falha_de_rede_mas_nao_em_404(self, monkeypatch):
        attempts = {"rede": 0, "404": 0}

        def urlopen(request, timeout):
            if "404" in request.full_url:
                attempts["404"] += 1
                raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)
            attempts["rede"] += 1
            if attempts["rede"] < 3:
                raise urllib.error.URLError("timeout")
            return io.BytesIO(b"ok")

        monkeypatch.setattr(reference.urllib.request, "urlopen", urlopen)
        monkeypatch.setattr(reference.time, "sleep", lambda s: None)
        assert reference.http_get("https://exemplo/rede") == b"ok"
        assert attempts["rede"] == 3
        with pytest.raises(reference.NotFoundError):
            reference.http_get("https://exemplo/404")
        assert attempts["404"] == 1


class TestUsoNoLoad:
    @pytest.fixture
    def db_path(self, tmp_path: Path) -> Path:
        return tmp_path / "hospital.db"

    def _dims(self, db_path: Path) -> tuple[list, list]:
        with closing(sqlite3.connect(db_path)) as conn:
            hospitais = conn.execute("SELECT id_hospital, nome_hospital FROM Dim_Hospital").fetchall()
            cids = conn.execute(
                "SELECT id_cid, descricao_cid, id_categoria, numero_capitulo FROM Dim_CID ORDER BY id_cid"
            ).fetchall()
        return hospitais, cids

    def test_preenche_nomes_e_descricoes(self, make_row, run_transform, trusted_path, db_path, reference_dir):
        run_transform([make_row(DIAG_PRINC="J189"), make_row(DIAG_PRINC="N182"), make_row(DIAG_PRINC="Z999")])
        reference_dir.mkdir()
        reference.parse_cid10_zip(cid10_zip()).to_parquet(reference_dir / reference.CID10_PATH.name)
        pd.DataFrame([{
            "id_hospital": "2025752", "nome_hospital": "SANTA CASA DE SANTOS", "razao_social": None,
            "id_municipio_cnes": "354850", "consultado_em_utc": "2026-11-02T00:00:00+00:00",
        }]).to_parquet(reference_dir / reference.HOSPITALS_PATH.name)

        load.load(trusted_path, db_path)
        hospitais, cids = self._dims(db_path)

        assert hospitais == [("2025752", "SANTA CASA DE SANTOS")]
        assert cids[0] == ("J189", "Pneumonia não especificada", "J18", 10)
        # Subcategoria criada depois de 2008: descrição da categoria, com nota
        assert cids[1] == (
            "N182", "Insuficiência renal crônica (subcategoria N182 ausente da tabela CID-10 de 2008)", "N18", 14
        )
        assert cids[2] == ("Z999", None, None, None)  # categoria inexistente na tabela

    def test_sem_dados_de_referencia_a_carga_segue_com_nulos(self, make_row, run_transform, trusted_path, db_path, caplog):
        run_transform([make_row()])
        load.load(trusted_path, db_path)
        hospitais, cids = self._dims(db_path)
        assert hospitais == [("2025752", None)]
        assert cids == [("J189", None, None, None)]
        assert "rode python -m src.reference" in caplog.text
