"""Stažení platných katastrálních území z číselníku ČÚZK."""

import csv
from datetime import datetime
import io
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Optional
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import requests

URL_CISELNIKU = "https://services.cuzk.cz/sestavy/cis/UI_KATASTRALNI_UZEMI.zip"
CILOVY_SOUBOR = Path(__file__).resolve().parent.parent / "tabulky_nove" / "UI_KATASTRALNI_UZEMI.csv"
SOUBOR_V_ZIP = "UI_KATASTRALNI_UZEMI.csv"
HLAVICKA_KU = ["KOD", "NAZEV", "OBEC_KOD"]


def stahni_katastralni_uzemi() -> int:
    """Ověří celý číselník a atomicky uloží pouze aktuálně platné KÚ."""
    time_now = datetime.now(ZoneInfo("Europe/Prague")).replace(tzinfo=None)

    with requests.get(URL_CISELNIKU, timeout=(15, 120)) as odpoved:
        odpoved.raise_for_status()
        obsah = odpoved.content
    with ZipFile(io.BytesIO(obsah)) as archiv:
        data = archiv.read(SOUBOR_V_ZIP).decode("windows-1250")
    reader = csv.DictReader(io.StringIO(data, newline=""), delimiter=";", strict=True)
    povinne = HLAVICKA_KU + ["PLATI_OD", "PLATI_DO"]
    if not set(povinne).issubset(reader.fieldnames or []):
        raise ValueError("Číselník KÚ nemá požadované sloupce.")

    radky = []
    kody = set()
    for row in reader:
        # None in row kontroluje, jestli nemá radek vice sloupců než hlavička.
        if None in row or any(hodnota is None for hodnota in row.values()):
            raise ValueError(f"Číselník KÚ, řádek {reader.line_num}: nesprávný počet sloupců.")
        hodnoty = {pole: row[pole].strip() for pole in povinne}
        if not hodnoty["NAZEV"] or any(
            not re.fullmatch(r"[0-9]{6}", hodnoty[pole]) for pole in ("KOD", "OBEC_KOD")
        ):
            raise ValueError(f"Číselník KÚ, řádek {reader.line_num}: neplatný kód nebo název.")
        if hodnoty["KOD"] in kody:
            raise ValueError(f"Číselník KÚ: duplicitní KOD {hodnoty['KOD']}.")
        kody.add(hodnoty["KOD"])
    
        try:
            plati_od = datetime.strptime(hodnoty["PLATI_OD"], "%d.%m.%Y %H:%M:%S")
            plati_do = None
            if hodnoty["PLATI_DO"]:
                plati_do = datetime.strptime(hodnoty["PLATI_DO"], "%d.%m.%Y %H:%M:%S")
        except ValueError as chyba:
            raise ValueError(f"Číselník KÚ, řádek {reader.line_num}: neplatné datum.") from chyba
        if plati_do is not None and plati_do <= plati_od:
            raise ValueError(f"Číselník KÚ, řádek {reader.line_num}: neplatný interval platnosti.")
        if plati_od <= time_now and (plati_do is None or time_now < plati_do):
            radky.append({pole: hodnoty[pole] for pole in HLAVICKA_KU})

    if not radky:
        raise ValueError("Číselník KÚ neobsahuje žádná platná katastrální území.")

    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=CILOVY_SOUBOR.parent, delete=False,
        ) as soubor:
            docasna_cesta = soubor.name
            writer = csv.DictWriter(soubor, fieldnames=HLAVICKA_KU)
            writer.writeheader()
            writer.writerows(radky)
        os.replace(docasna_cesta, CILOVY_SOUBOR)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)
    print(f"Uloženo {len(radky)} platných KÚ do {CILOVY_SOUBOR}.", flush=True)
    return len(radky)


if __name__ == "__main__":
    try:
        stahni_katastralni_uzemi()
    except Exception as chyba:
        print(f"Stažení KÚ selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
