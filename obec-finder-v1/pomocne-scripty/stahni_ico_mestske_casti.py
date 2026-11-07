"""Stažení IČO a názvů pražských městských částí z ARES."""
import csv
import os
from pathlib import Path
import sys
import tempfile
from typing import Optional

import requests

URL_ARES = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/"
CILOVY_SOUBOR = Path(__file__).resolve().parent.parent / "tabulky_nove" / "ico_mestske_casti.csv"


def stahni_ico_mestske_casti() -> int:
    """Stáhne IČO a názvy městských částí a uloží CSV."""
    # V ROS má městská část Prahy právní formu 811
    with requests.post(
        URL_ARES + "ekonomicke-subjekty/vyhledat",
        json={"pravniFormaRos": ["811"], "start": 0, "pocet": 100},
        timeout=(15, 120),
    ) as odpoved:
        odpoved.raise_for_status()
        vysledek = odpoved.json()

    subjekty = vysledek["ekonomickeSubjekty"]
    if not subjekty or len(subjekty) != vysledek["pocetCelkem"]:
        raise ValueError("ARES nevrátil úplný seznam pražských městských částí.")

    radky = []
    for subjekt in subjekty:
        # Filtr nevylučuje zaniklé subjekty.
        if subjekt.get("datumZaniku"):
            continue
        ico = str(subjekt["ico"] if subjekt["ico"] is not None else "").strip()
        # Pro budoucí párování s názvy městských částí.
        nazev = (subjekt["obchodniJmeno"] or "").strip().removeprefix("Městská část ").strip()
        radky.append([ico, nazev])

    if not radky:
        raise ValueError("ARES nevrátil žádné nezaniklé pražské městské části.")

    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="",
            dir=os.path.dirname(CILOVY_SOUBOR) or ".", delete=False,
        ) as soubor:
            docasna_cesta = soubor.name
            writer = csv.writer(soubor, delimiter=";")
            writer.writerow(["ico", "nazev"])
            writer.writerows(radky)
        os.replace(docasna_cesta, CILOVY_SOUBOR)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)

    return len(radky)


if __name__ == "__main__":
    try:
        pocet = stahni_ico_mestske_casti()
    except Exception as chyba:
        print(f"Stažení IČO pražských městských částí z ARES selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
    print(f"ARES: uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
