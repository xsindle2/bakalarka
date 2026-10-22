"""Stažení aktuálního číselníku krajů z ČSÚ do tabulky CIS0100_CS.csv."""
import csv
import io
import os
import tempfile
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import requests

URL_EXPORTU = "https://apl2.czso.cz/iSMS/do_cis_export"
CILOVY_SOUBOR = "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky_temp/CIS0100_CS.csv"
HLAVICKA_KRAJU = [
    "kodjaz", "akrcis", "kodcis", "chodnota", "zkrtext", "text",
    "admplod", "admnepo", "cznuts", "kod_ruian", "zkrkraj",
]


def stahni_kraje() -> int:
    """Ověří stažené CSV, uloží ho a vrátí počet řádků."""
    parametry = {
        "kodcis": "100",       # Číselník krajů
        "typdat": "0",         # Číselník ne vazba mezi číselníky
        "cisjaz": "203",       # Čeština
        "format": "2",         # CSV
        "datpohl": datetime.now(ZoneInfo("Europe/Prague")).strftime("%d.%m.%Y"),
    }
    with requests.get(URL_EXPORTU, params=parametry, timeout=(15, 120)) as odpoved:
        odpoved.raise_for_status()
        obsah = odpoved.content

    reader = csv.reader(io.StringIO(obsah.decode("utf-8"), newline=""), strict=True)
    if next(reader, None) != HLAVICKA_KRAJU:
        raise ValueError("ČSÚ vrátilo neočekávanou hlavičku číselníku krajů.")

    pocet_radku = 0
    for radek in reader:
        if len(radek) != len(HLAVICKA_KRAJU) or not any(hodnota.strip() for hodnota in radek):
            raise ValueError("Číselník krajů obsahuje neplatný řádek.")
        pocet_radku += 1
    if pocet_radku == 0:
        raise ValueError("Číselník krajů neobsahuje žádná data.")

    # Atomicka vymena dat stareho a noveho souboru
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
        pocet = stahni_kraje()
    except Exception as chyba:
        print(f"Stažení krajů selhalo: {chyba}")
        raise SystemExit(1)
    print(f"Uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
