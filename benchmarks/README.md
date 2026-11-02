# Benchmark do leitor DBF

Compara o `src/dbf_reader.py` com outros leitores de DBF e mede a descompressão do `.dbc`.

```bash
pip install -r requirements.txt -r benchmarks/requirements.txt
python -m benchmarks.run caminho/RDSP2501.dbf      # leitores de .dbf
python -m benchmarks.run data/raw/RDSP2501.dbc     # descompressores de .dbc
```

Cada leitor roda num processo novo, com o arquivo já no cache do sistema operacional. O tempo é a mediana de 5 execuções (2 nos leitores lentos), e a memória é o pico do processo. O leitor do PySUS (GPL-3.0) é baixado do GitHub, num commit fixo, na primeira execução; ele não é copiado para este repositório.

## Resultado (02/11/2026)

Windows 11, 16 threads, Python 3.14.8, numpy 2.5.4, pandas 3.0.6, pyarrow 26.0.0. Arquivos `RDSP2501` (167 MB, 238.144 linhas × 113 colunas) e `RDSP2505` (182 MB, 259.070 × 114), descompactados dos `.dbc` publicados pelo DATASUS.

| Leitor | RDSP2501 | RDSP2505 | Pico de RAM | Observação |
|---|---|---|---|---|
| `prototipo_arrow` | 1,3 s | 1,3 s | 0,7 GB | decodifica em Arrow; mesma saída do projeto |
| `duckdb_spatial` (GDAL) | 8,9 s | 9,4 s | 0,4 GB | |
| **`projeto`** | **10,2 s** | **12,3 s** | **0,7 GB** | |
| `pysus` | 11,1 s | 10,5 s | 1,6 GB | tudo como texto, sem validação estrutural |
| `simpledbf` | 14,0 s | 16,8 s | 1,5 GB | |
| `pyogrio` | 26,1 s | 28,8 s | 0,4 GB | |
| `dbfread` | 30,9 s | 33,4 s | 4,0 GB | |
| `dbf_furman` | 113,5 s | | 1,6 GB | |
| `projeto_colunas` (12 colunas) | 1,2 s | 1,3 s | 0,3 GB | |
| `pysus_colunas` (12 colunas) | 1,4 s | 1,5 s | 0,6 GB | |
| `prototipo_arrow_colunas` (12 colunas) | 0,2 s | 0,2 s | 0,5 GB | |

Para comparar: ler o Parquet já convertido leva 0,08 s.

O `fastdbf` (Rust) leu em 1,8 s, mas a saída está errada. Ele posiciona cada campo pelo deslocamento gravado no descritor (bytes 12–15), que nos arquivos do DATASUS é sempre zero. Todo campo é lido a partir do byte 0 do registro, e todas as linhas saem iguais.

Descompressão do `RDSP2501.dbc` (18 MB → 167 MB, saída idêntica byte a byte nos três):

| Descompressor | Tempo |
|---|---|
| `pyreaddbc` (Python 3.13) | 0,7 s |
| `datasus_dbc.decompress_bytes` (em memória, usado pelo extract) | 1,4 s |
| `datasus_dbc.decompress` (grava em disco) | 45 s |

O `decompress` do `datasus-dbc` 0.1.3 passa o arquivo ao descompressor sem `BufReader`, e a leitura é feita byte a byte.

## Limitações da medição

- Uma máquina e um sistema operacional só. Sem medição com o cache frio; como a leitura é limitada pela CPU, o efeito deve ser pequeno (ler 167 MB do SSD leva ~0,1 s).
- Os leitores não devolvem a mesma coisa: o projeto e o dbfread tipam números e datas, o PySUS devolve tudo como texto, e o pyogrio, o DuckDB e o protótipo devolvem tabelas Arrow.
- O DATASUS republica os arquivos, então um `RDSP2501` baixado depois pode não ser o mesmo arquivo.
