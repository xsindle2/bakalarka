"""Stažení vazeb částí obcí a obcí na adresní PSČ z České pošty."""
import csv
import io
import os
import tempfile
import zipfile
from typing import Optional

import requests

URL_EXPORTU = "https://www.ceskaposta.cz/documents/d/guest/csv_cobce_psc-zip?download=true"
CILOVY_SOUBOR = "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky_temp/zv_cobce_psc.csv"
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

    reader = csv.reader(
        io.StringIO(obsah.decode("windows-1250"), newline=""), delimiter=";", strict=True,
    )
    if next(reader, None) != HLAVICKA_PSC:
        raise ValueError(f"Česká pošta vrátila neočekávanou hlavičku tabulky {CILOVY_SOUBOR}.")

    pocet_radku = 0
    for radek in reader:
        if len(radek) != len(HLAVICKA_PSC) or not any(hodnota.strip() for hodnota in radek):
            raise ValueError(f"Tabulka {CILOVY_SOUBOR} obsahuje neplatný řádek.")
        pocet_radku += 1
    if pocet_radku == 0:
        raise ValueError(f"Tabulka {CILOVY_SOUBOR} neobsahuje žádná data.")

    # Stejny zpusob ukladani jakko v stahni_csu.py
    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(dir=os.path.dirname(CILOVY_SOUBOR) or ".", delete=False) as soubor:
            docasna_cesta = soubor.name
            soubor.write(obsah)
        os.replace(docasna_cesta, CILOVY_SOUBOR)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)

    return pocet_radku


if __name__ == "__main__":
    try:
        pocet = stahni_psc()
    except Exception as chyba:
        print(f"Stažení PSČ selhalo: {chyba}")
        raise SystemExit(1)
    print(f"Uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
