"""Caminhos e constantes compartilhados pelas etapas do pipeline."""

from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]

DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
CONVERTED_DIR: Path = DATA_DIR / "converted"
TRUSTED_DIR: Path = DATA_DIR / "trusted"
DATABASE_PATH: Path = DATA_DIR / "database" / "hospital_data.db"

LOG_DIR: Path = PROJECT_ROOT / "logs"

# Códigos IBGE de 6 dígitos (sem o dígito verificador), como aparecem no SIH e no CNES
BAIXADA_SANTISTA: dict[str, str] = {
    "350635": "Bertioga",
    "351350": "Cubatão",
    "351870": "Guarujá",
    "352210": "Itanhaém",
    "353110": "Mongaguá",
    "353760": "Peruíbe",
    "354100": "Praia Grande",
    "354850": "Santos",
    "355100": "São Vicente",
}
