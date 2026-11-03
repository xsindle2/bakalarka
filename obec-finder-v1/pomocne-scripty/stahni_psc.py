"""Stažení vazeb částí obcí a obcí na adresní PSČ z České pošty."""
import csv
import io
import os
from pathlib import Path
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

    reader = csv.reader(
        io.StringIO(obsah.decode("windows-1250"), newline=""), delimiter=";", strict=True,
    )
    if next(reader, None) != HLAVICKA_PSC:
        raise ValueError(f"Česká pošta vrátila neočekávanou hlavičku tabulky {CILOVY_SOUBOR}.")

    radky = []
    for radek in reader:
        if len(radek) != len(HLAVICKA_PSC) or not any(hodnota.strip() for hodnota in radek):
            raise ValueError(f"Tabulka {CILOVY_SOUBOR} obsahuje neplatný řádek.")
        radek = [hodnota.strip() for hodnota in radek]
        for index, sloupec in enumerate(HLAVICKA_PSC):
            if sloupec == "psc":
                radek[index] = radek[index].replace(" ", "")
            if sloupec.startswith("kod") or sloupec == "psc":
                radek[index] = radek[index].lstrip("0")
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
            writer = csv.writer(soubor, delimiter=";")
            writer.writerow(HLAVICKA_PSC)
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
        print(f"Stažení PSČ selhalo: {chyba}")
        raise SystemExit(1)
    print(f"Uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
