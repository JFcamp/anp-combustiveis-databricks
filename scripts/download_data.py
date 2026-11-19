"""Baixa os CSVs mensais de preços de combustíveis da ANP e, opcionalmente, envia ao Volume.

Uso:
    python scripts/download_data.py --start 2025-01 --end 2025-12
    python scripts/download_data.py --start 2025-01 --end 2025-12 \
        --upload-to /Volumes/workspace/anp_combustiveis/raw/anp

O upload usa o Databricks CLI (`databricks fs cp`), que lê credenciais do perfil do CLI
ou das variáveis DATABRICKS_HOST / DATABRICKS_TOKEN. Nenhuma credencial fica no código.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

PAGE_URL = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "serie-historica-de-precos-de-combustiveis"
)
# A ANP responde 403 para clientes sem User-Agent de navegador.
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
GROUPS = ("gasolina-etanol", "diesel-gnv", "glp")
LINK_RE = re.compile(r'href="([^"]*/shpc/dsan/(\d{4})/([^"/]+))"')


def http_get(url: str, timeout: int = 120) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_link(url: str, year: str, name: str) -> dict | None:
    """Extrai grupo e mês do nome do arquivo. Os padrões mudam entre anos.

    2023-2025: precos-gasolina-etanol-01.csv
    2026:      01-dados-abertos-precos-gasolina-etanol.csv, 10-glp.csv,
               06-dados-abertos-precos-2026-06-diesel-gnv.csv
    """
    lowered = name.lower()
    group = next((g for g in GROUPS if g in lowered), None)
    if group is None and "cados-abertos-preco-gasolina-etanol" in lowered:
        group = "gasolina-etanol"  # typo existente no nome de um arquivo da ANP
    if group is None:
        return None
    match = re.match(r"^(\d{2})-", lowered) or re.search(r"-(\d{2})(?:\.csv)?$", lowered)
    if match is None:
        return None
    month = match.group(1)
    if not 1 <= int(month) <= 12:
        return None
    return {
        "url": url,
        "year": year,
        "month": month,
        "group": group,
        "filename": f"{group}-{year}-{month}.csv",
    }


def discover_links() -> list[dict]:
    html = http_get(PAGE_URL).decode("utf-8", errors="replace")
    found: dict[str, dict] = {}
    for url, year, name in LINK_RE.findall(html):
        item = parse_link(url, year, name)
        if item:
            found[item["filename"]] = item
    return sorted(found.values(), key=lambda i: (i["year"], i["month"], i["group"]))


def in_range(item: dict, start: str, end: str) -> bool:
    return start <= f"{item['year']}-{item['month']}" <= end


def upload(local: Path, target_dir: str, year: str) -> None:
    cli = shutil.which("databricks")
    if cli is None:
        sys.exit(
            "Databricks CLI não encontrado. Instale: https://docs.databricks.com/dev-tools/cli/install"
        )
    destination = f"dbfs:{target_dir.rstrip('/')}/{year}/{local.name}"
    subprocess.run([cli, "fs", "cp", str(local), destination, "--overwrite"], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--start", default="2025-01", help="Mês inicial AAAA-MM (padrão 2025-01)")
    parser.add_argument("--end", default="2025-12", help="Mês final AAAA-MM (padrão 2025-12)")
    parser.add_argument(
        "--groups", nargs="+", default=["gasolina-etanol", "diesel-gnv"], choices=GROUPS
    )
    parser.add_argument("--out", default="data/raw", help="Pasta local de saída")
    parser.add_argument(
        "--upload-to", help="Caminho do Volume, ex.: /Volumes/workspace/anp_combustiveis/raw/anp"
    )
    parser.add_argument("--force", action="store_true", help="Baixa de novo arquivos já existentes")
    parser.add_argument("--dry-run", action="store_true", help="Só lista os arquivos")
    args = parser.parse_args()

    items = [
        i
        for i in discover_links()
        if i["group"] in args.groups and in_range(i, args.start, args.end)
    ]
    if not items:
        sys.exit("Nenhum arquivo encontrado para o período e os grupos informados.")
    print(f"{len(items)} arquivo(s) no período {args.start} a {args.end}")

    for item in items:
        local = Path(args.out) / item["year"] / item["filename"]
        if args.dry_run:
            print(f"[dry-run] {item['url']} -> {local}")
            continue
        if local.exists() and not args.force:
            print(f"já existe: {local}")
        else:
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(http_get(item["url"]))
            print(f"baixado: {local} ({local.stat().st_size / 1e6:.1f} MB)")
        if args.upload_to:
            upload(local, args.upload_to, item["year"])
            print(f"enviado: {args.upload_to}/{item['year']}/{local.name}")


if __name__ == "__main__":
    main()
