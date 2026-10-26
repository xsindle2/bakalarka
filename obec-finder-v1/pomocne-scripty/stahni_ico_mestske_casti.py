"""Stažení IČO a názvů pražských městských částí z ARES."""
import csv
import os
import tempfile
from typing import Optional

import requests

URL_ARES = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/"
CILOVY_SOUBOR = "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky_temp/ico_mestske_casti.csv"


def stahni_ico_mestske_casti() -> int:
    """Ověří IČO a názvy městských částí, uloží CSV a vrátí počet řádků."""
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
    pouzita_ico = set()
    pouzite_nazvy = set()
    for subjekt in subjekty:
        # Filtr nevylučuje zaniklé subjekty.
        if subjekt.get("datumZaniku"):
            continue
        ico = subjekt["ico"]
        if len(ico) != 8 or not ico.isdigit():
            raise ValueError(f"ARES vrátil neplatné IČO: {ico}")
        # Pro budoucí párování s názvy městských částí.
        nazev = subjekt["obchodniJmeno"].removeprefix("Městská část ").strip()
        if not nazev:
            raise ValueError(f"U IČO {ico} chybí název městské části.")
        if ico in pouzita_ico or nazev in pouzite_nazvy:
            raise ValueError(f"Duplicitní IČO nebo název městské části: {ico}, {nazev}")

        radky.append([ico, nazev])
        pouzita_ico.add(ico)
        pouzite_nazvy.add(nazev)

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
        print(f"Stažení IČO pražských městských částí z ARES selhalo: {chyba}")
        raise SystemExit(1)
    print(f"ARES: uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
