"""Benchmark dos leitores de DBF e dos descompressores de DBC.

Cada leitor roda num processo novo (o pico de memória de um não contamina o outro),
com o arquivo já no cache do sistema operacional. Imprime uma tabela em Markdown.

Uso, a partir da raiz do projeto:
    python -m benchmarks.run data/raw/RDSP2501.dbf              # leitores de .dbf
    python -m benchmarks.run data/raw/RDSP2501.dbc              # descompressores de .dbc
    python -m benchmarks.run arquivo.dbf --leitores projeto pysus --repeticoes 5
"""

from __future__ import annotations

import argparse
import gc
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

from benchmarks import readers


def _peak_memory_mb() -> float:
    try:
        import psutil
        info = psutil.Process().memory_info()
        return getattr(info, "peak_wset", info.rss) / 2**20  # peak_wset só existe no Windows
    except ImportError:
        import resource  # Linux: ru_maxrss em KB
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _shape(out) -> str:
    if isinstance(out, bytes):
        return f"{len(out) / 2**20:,.0f} MB"
    rows, cols = (out.num_rows, out.num_columns) if hasattr(out, "num_rows") else out.shape
    return f"{rows:,} x {cols}"


def run_one(name: str, path: str, repeat: int) -> dict:
    fn = getattr(readers, name)
    times = []
    for _ in range(repeat):
        gc.collect()
        started = time.perf_counter()
        out = fn(path)
        times.append(time.perf_counter() - started)
        shape = _shape(out)
        del out
    return {"leitor": name, "tempos": times, "formato": shape, "pico_mb": _peak_memory_mb()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("arquivos", nargs="+", type=Path)
    parser.add_argument("--leitores", nargs="+", help="padrão: todos os do tipo do arquivo")
    parser.add_argument("--repeticoes", type=int, default=5, help="repetições dos leitores rápidos (padrão: 5)")
    parser.add_argument("--repeticoes-lentos", type=int, default=2, help="repetições dos leitores lentos (padrão: 2)")
    parser.add_argument("--um", nargs=3, metavar=("LEITOR", "ARQUIVO", "N"), help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.um:
        name, path, n = args.um
        print(json.dumps(run_one(name, path, int(n))))
        return 0

    print("| arquivo | leitor | mediana (s) | mín (s) | n | pico de RAM | saída |")
    print("|---|---|---|---|---|---|---|")
    for path in args.arquivos:
        path.read_bytes()  # aquece o cache do sistema operacional
        default = readers.DBC_READERS if path.suffix.lower() == ".dbc" else readers.DBF_READERS
        for name in args.leitores or default:
            n = args.repeticoes_lentos if name in readers.SLOW else args.repeticoes
            proc = subprocess.run(
                [sys.executable, "-m", "benchmarks.run", "x", "--um", name, str(path), str(n)],
                capture_output=True, text=True,
            )
            if proc.returncode:
                error = (proc.stderr.strip().splitlines() or ["?"])[-1]
                print(f"| {path.name} | {name} | erro: {error[:80]} | | | | |")
                continue
            r = json.loads(proc.stdout.strip().splitlines()[-1])
            t = r["tempos"]
            print(f"| {path.name} | {name} | {statistics.median(t):.2f} | {min(t):.2f} | {len(t)} "
                  f"| {r['pico_mb'] / 1024:.1f} GB | {r['formato']} |", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
