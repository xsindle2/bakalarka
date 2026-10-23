"""Stažení číselníků krajů a okresů a vazby obcí na okresy z ČSÚ."""
import csv
import io
import os
import tempfile
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import requests

URL_EXPORTU = "https://apl2.czso.cz/iSMS/do_cis_export"
CILOVA_SLOZKA = "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky_temp"
HLAVICKA_KRAJU = [
    "kodjaz", "akrcis", "kodcis", "chodnota", "zkrtext", "text",
    "admplod", "admnepo", "cznuts", "kod_ruian", "zkrkraj",
]

HLAVICKA_OKRESU = [
    "kodjaz", "akrcis", "kodcis", "chodnota", "zkrtext", "text",
    "admplod", "admnepo", "cznuts", "okres_lau", "kraj_1960", "kod_ruian",
]

HLAVICKA_VAZBY = [
    "kodjaz", "typvaz", "akrcis1", "kodcis1", "chodnota1", "text1",
    "akrcis2", "kodcis2", "chodnota2", "text2",
]


def stahni_tabulku(
    kodcis: str, cilovy_soubor: str, hlavicka: list[str], cisvaz: Optional[str] = None,
) -> int:
    """Ověří stažené CSV, uloží ho a vrátí počet řádků."""
    parametry = {
        "kodcis": kodcis,      # 100 = kraje, 101 = okresy, 43 = obce
        "typdat": "1" if cisvaz else "0",  # 0 = číselník, 1 = vazba
        "cisjaz": "203",       # Čeština
        "format": "2",         # CSV
        "datpohl": datetime.now(ZoneInfo("Europe/Prague")).strftime("%d.%m.%Y"),
    }
    if cisvaz:
        parametry["cisvaz"] = cisvaz

    with requests.get(URL_EXPORTU, params=parametry, timeout=(15, 120)) as odpoved:
        odpoved.raise_for_status()
        obsah = odpoved.content

    reader = csv.reader(io.StringIO(obsah.decode("utf-8"), newline=""), strict=True)
    if next(reader, None) != hlavicka:
        raise ValueError(f"ČSÚ vrátilo neočekávanou hlavičku tabulky {cilovy_soubor}.")

    pocet_radku = 0
    for radek in reader:
        if len(radek) != len(hlavicka) or not any(hodnota.strip() for hodnota in radek):
            raise ValueError(f"Tabulka {cilovy_soubor} obsahuje neplatný řádek.")
        pocet_radku += 1
    if pocet_radku == 0:
        raise ValueError(f"Tabulka {cilovy_soubor} neobsahuje žádná data.")

    # Atomicka vymena dat stareho a noveho souboru
    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(dir=os.path.dirname(cilovy_soubor) or ".", delete=False) as soubor:
            docasna_cesta = soubor.name
            soubor.write(obsah)
        os.replace(docasna_cesta, cilovy_soubor)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)

    return pocet_radku


if __name__ == "__main__":
    tabulky = [
        ("100", "CIS0100_CS.csv", HLAVICKA_KRAJU, None),
        ("101", "CIS0101_CS.csv", HLAVICKA_OKRESU, None),
        ("43", "VAZ0043_0101_CS.csv", HLAVICKA_VAZBY, "101_333"),
    ]
    try:
        for kodcis, nazev_souboru, hlavicka, cisvaz in tabulky:
            cilovy_soubor = os.path.join(CILOVA_SLOZKA, nazev_souboru)
            pocet = stahni_tabulku(kodcis, cilovy_soubor, hlavicka, cisvaz)
            print(f"Uloženo {pocet} záznamů do {cilovy_soubor}.")
    except Exception as chyba:
        print(f"Stažení tabulek selhalo: {chyba}")
        raise SystemExit(1)
