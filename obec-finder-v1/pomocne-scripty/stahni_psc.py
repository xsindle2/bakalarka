"""Stažení vazeb částí obcí a obcí na adresní PSČ z České pošty."""
import csv
import io
import os
from pathlib import Path
import sys
import tempfile
import zipfile
from typing import Optional

import requests

URL_EXPORTU = "https://www.ceskaposta.cz/documents/d/guest/csv_cobce_psc-zip?download=true"
CILOVY_SOUBOR = Path(__file__).resolve().parent.parent / "tabulky_nove" / "zv_cobce_psc.csv"
HLAVICKA_PSC = [
    "kodcobce", "nazcobce", "psc", "kodobce", "nazobce", "kodokresu",
    "nazokresu", "kodkraj", "nazevkraj", "kodmomc", "nazmomc",
    "kodpobvod", "nazpobvod",
]


def stahni_psc() -> int:
    """Ověří CSV ze staženého ZIPu, uloží ho a vrátí počet řádků."""
    with requests.get(URL_EXPORTU, timeout=(15, 120)) as odpoved:
        odpoved.raise_for_status()
        obsah_zip = odpoved.content

    # Archiv otevřeme v paměti; ZIP se neukládá ani nerozbaluje do složky.
    with zipfile.ZipFile(io.BytesIO(obsah_zip)) as archiv:
        obsah = archiv.read("zv_cobce_psc.csv")

    reader = csv.DictReader(
        io.StringIO(obsah.decode("windows-1250"), newline=""), delimiter=";", strict=True,
    )
    if not set(HLAVICKA_PSC).issubset(reader.fieldnames or []):
        raise ValueError(f"Česká pošta vrátila neočekávanou hlavičku tabulky {CILOVY_SOUBOR}.")

    radky = []
    for radek in reader:
        if None in radek or any(hodnota is None for hodnota in radek.values()):
            raise ValueError(f"Tabulka {CILOVY_SOUBOR}, řádek {reader.line_num}: nesprávný počet sloupců.")
        radek = {sloupec: radek[sloupec].strip() for sloupec in HLAVICKA_PSC}
        if not any(radek.values()):
            raise ValueError(f"Tabulka {CILOVY_SOUBOR} obsahuje neplatný řádek.")
        for sloupec in HLAVICKA_PSC:
            if sloupec == "psc":
                radek[sloupec] = radek[sloupec].replace(" ", "")
            if sloupec.startswith("kod") or sloupec == "psc":
                radek[sloupec] = radek[sloupec].lstrip("0")
        radky.append(radek)
    if not radky:
        raise ValueError(f"Tabulka {CILOVY_SOUBOR} neobsahuje žádná data.")

    # Stejny zpusob ukladani jakko v stahni_csu.py
    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="windows-1250", newline="",
            dir=os.path.dirname(CILOVY_SOUBOR) or ".", delete=False,
        ) as soubor:
            docasna_cesta = soubor.name
            writer = csv.DictWriter(soubor, fieldnames=HLAVICKA_PSC, delimiter=";")
            writer.writeheader()
            writer.writerows(radky)
        os.replace(docasna_cesta, CILOVY_SOUBOR)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)

    return len(radky)


if __name__ == "__main__":
    try:
        pocet = stahni_psc()
    except Exception as chyba:
        print(f"Stažení PSČ selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
    print(f"Uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
