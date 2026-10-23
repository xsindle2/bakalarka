"""Stažení aktuálních číselníků krajů, okresů a obcí z ČSÚ."""
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

HLAVICKA_OBCI = [
    "kodjaz", "akrcis", "kodcis", "chodnota", "zkrtext", "text",
    "admplod", "admnepo", "obec_ul", "ob_ul_cor", "sm_rozsah", "sm_typ",
    "kodobce_h", "domin_cobc", "porcobec_o",
]


def stahni_ciselnik(kodcis: str, cilovy_soubor: str, hlavicka: list[str]) -> int:
    """Ověří stažené CSV, uloží ho a vrátí počet řádků."""
    parametry = {
        "kodcis": kodcis,      # 100 = kraje, 101 = okresy, 43 = obce
        "typdat": "0",         # Číselník ne vazba mezi číselníky
        "cisjaz": "203",       # Čeština
        "format": "2",         # CSV
        "datpohl": datetime.now(ZoneInfo("Europe/Prague")).strftime("%d.%m.%Y"),
    }
    with requests.get(URL_EXPORTU, params=parametry, timeout=(15, 120)) as odpoved:
        odpoved.raise_for_status()
        obsah = odpoved.content

    reader = csv.reader(io.StringIO(obsah.decode("utf-8"), newline=""), strict=True)
    if next(reader, None) != hlavicka:
        raise ValueError(f"ČSÚ vrátilo neočekávanou hlavičku číselníku {kodcis}.")

    pocet_radku = 0
    for radek in reader:
        if len(radek) != len(hlavicka) or not any(hodnota.strip() for hodnota in radek):
            raise ValueError(f"Číselník {kodcis} obsahuje neplatný řádek.")
        pocet_radku += 1
    if pocet_radku == 0:
        raise ValueError(f"Číselník {kodcis} neobsahuje žádná data.")

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
    ciselniky = [
        ("100", "CIS0100_CS.csv", HLAVICKA_KRAJU),
        ("101", "CIS0101_CS.csv", HLAVICKA_OKRESU),
        ("43", "CIS0043_CS.csv", HLAVICKA_OBCI),
    ]
    try:
        for kodcis, nazev_souboru, hlavicka in ciselniky:
            cilovy_soubor = os.path.join(CILOVA_SLOZKA, nazev_souboru)
            pocet = stahni_ciselnik(kodcis, cilovy_soubor, hlavicka)
            print(f"Uloženo {pocet} záznamů do {cilovy_soubor}.")
    except Exception as chyba:
        print(f"Stažení číselníků selhalo: {chyba}")
        raise SystemExit(1)
