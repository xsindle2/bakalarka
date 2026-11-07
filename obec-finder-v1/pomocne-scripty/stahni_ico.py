"""Stažení vazeb IČO na kódy obcí z filtrovaného JSON rozhraní MONITORu.

Používá stejné rozhraní jako webový prohlížeč číselníků MONITORu.
Výběr obcí a platnosti k datu spuštění provádí server.
"""

"""Poznamka pro me: pouzil jsem tento zdroj, protoze se zaznamy filtruji 
uz na serveru a nemusim tak stahovat obrovksy soubor jako pred tim.
Je nutne to delat pres monitor, protoze ares neobsahuje vazbu na ZUJ kod."""

import csv
import os
from pathlib import Path
import sys
import tempfile
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import requests

URL_EXPORTU = "https://monitor.statnipokladna.gov.cz/api/ciselniky/7/data"
CILOVY_SOUBOR = Path(__file__).resolve().parent.parent / "tabulky_nove" / "ico_obce.csv"


def stahni_ico() -> int:
    """Stáhne filtrované obce, sjednotí zápis a uloží CSV."""
    parametry = {
        "date": datetime.now(ZoneInfo("Europe/Prague")).strftime("%Y-%m-%d"),
        "filters": '{"druhuj_id":"4"}',  # 4 = obec
        "page": 1,
        "pageSize": 10000,
        "sortColumn": "ucjed_id", # řazení nepoužívám, ale server vyžaduje, aby bylo zadáno
        "sortDir": "asc",
    }
    with requests.get(URL_EXPORTU, params=parametry, timeout=(15, 120)) as odpoved:
        odpoved.raise_for_status()
        vysledek = odpoved.json()

    obce = vysledek["data"]
    if not obce or len(obce) != vysledek["page"]["totalElements"]:
        raise ValueError("MONITOR nevrátil úplný seznam obcí.")

    radky = []
    for obec in obce:
        ico = str(obec["ico"] if obec["ico"] is not None else "").strip()
        kod_obce = str(obec["zuj_id"] if obec["zuj_id"] is not None else "").strip().lstrip("0")
        nazev = (obec["ucjed_nazev"] or "").strip()
        radky.append([ico, kod_obce, nazev])

    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="",
            dir=os.path.dirname(CILOVY_SOUBOR) or ".", delete=False,
        ) as soubor:
            docasna_cesta = soubor.name
            writer = csv.writer(soubor, delimiter=";")
            writer.writerow(["ico", "kod_obce", "nazev"])
            writer.writerows(radky)
        os.replace(docasna_cesta, CILOVY_SOUBOR)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)

    return len(radky)


if __name__ == "__main__":
    try:
        pocet = stahni_ico()
    except Exception as chyba:
        print(f"Stažení IČO obcí z MONITOR JSON selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
    print(f"Uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
