"""Stažení interních ID portálu mesta.obce.cz podle kódů ZÚJ.

ID se čte z přesměrování - samotné profily úřadů se nestahují.
"""
import csv
import os
import re
import tempfile
import time
from typing import Optional

import requests

URL_VYHLEDAVANI = "https://mesta.obce.cz/vyhledat.asp"
VSTUPNI_SOUBORY = [
    "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky/VAZ0043_0101_CS.csv",
    "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky/VAZ0044_0043_CS.csv",
]
CILOVY_SOUBOR = "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky/mesta_obce_id.csv"
SOUBOR_LOGU = "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky/chybejici_mesta_obce.txt"
PAUZA = 0.15


def vytvor_slovnik_zuj() -> dict[str, str]:
    """Načte kódy a názvy obcí a městských částí z obou tabulek ČSÚ."""
    zuj_dict = {}
    for cesta in VSTUPNI_SOUBORY:
        with open(cesta, encoding="utf-8-sig", newline="") as soubor:
            reader = csv.DictReader(soubor)
            for radek in reader:
                zuj = radek["chodnota1"]
                nazev = radek["text1"].strip()
                zuj_dict[zuj] = nazev
    return zuj_dict


def stahni_ids() -> int:
    """Stáhne vazby, uloží CSV a log nenalezených kódů a vrátí počet vazeb."""
    zacatek = time.monotonic()
    zuj_map = vytvor_slovnik_zuj()
    print(f"Začínám hledat profily. Počet kódů ZÚJ: {len(zuj_map)}.", flush=True)
    vysledky = []
    nenalezene = []

    with requests.Session() as session:
        session.headers.update({"User-Agent": "MuniParser/1.0 (Bakalarska_prace)"})
        zpracovano = 0
        for zuj, nazev in zuj_map.items():
            zpracovano += 1
            try:
                with session.get(
                    URL_VYHLEDAVANI, params={"zuj": zuj},
                    allow_redirects=False, timeout=(10, 20),
                ) as odpoved:
                    odpoved.raise_for_status()
                    if odpoved.status_code in (301, 302, 303, 307, 308):
                        cesta = odpoved.headers.get("Location", "")
                        shoda = re.fullmatch(r"/zsu/vyhledat-([0-9]+)\.htm", cesta, re.IGNORECASE)
                        if not shoda:
                            raise ValueError(f"Neočekávané přesměrování pro ZÚJ {zuj}: {cesta}")
                        vysledky.append([zuj, shoda.group(1)])
                    else:
                        odpoved.encoding = "windows-1250"
                        if odpoved.status_code != 200 or "Nenalezeno" not in odpoved.text:
                            raise ValueError(f"Neočekávaná odpověď při hledání ZÚJ {zuj}.")
                        nenalezene.append(f"ZÚJ: {zuj} | Uzel: {nazev}")
            except requests.RequestException as chyba:
                raise RuntimeError(f"Stažení ZÚJ {zuj} ({nazev}) selhalo: {chyba}") from chyba

            if zpracovano % 100 == 0 or zpracovano == len(zuj_map):
                print(
                    f"Zpracováno {zpracovano}/{len(zuj_map)}. "
                    f"Nalezeno: {len(vysledky)}, nenalezeno: {len(nenalezene)}. "
                    f"Čas: {(time.monotonic() - zacatek) / 60:.1f} min.",
                    flush=True,
                )
            if zpracovano < len(zuj_map):
                time.sleep(PAUZA)

    if not vysledky:
        raise ValueError("Nebyl nalezen žádný profil. Původní CSV se nepřepíše.")

    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="",
            dir=os.path.dirname(CILOVY_SOUBOR) or ".", delete=False,
        ) as soubor:
            docasna_cesta = soubor.name
            writer = csv.writer(soubor, delimiter=";")
            writer.writerow(["zuj", "id_mesta_obce"])
            writer.writerows(vysledky)
            
        # Zápis logu nenalezených kódů
        with open(SOUBOR_LOGU, "w", encoding="utf-8") as soubor:
            soubor.write(f"Pro tyto kódy ZÚJ nebyl nalezen odpovídající profil ({len(nenalezene)}):\n")
            for zaznam in nenalezene:
                soubor.write(zaznam + "\n")
        os.replace(docasna_cesta, CILOVY_SOUBOR)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)

    print(f"Uloženo {len(vysledky)} vazeb do {CILOVY_SOUBOR}.")
    print(f"Nenalezené kódy: {SOUBOR_LOGU}")
    print(f"Celkový čas: {(time.monotonic() - zacatek) / 60:.1f} min.")
    return len(vysledky)


if __name__ == "__main__":
    try:
        stahni_ids()
    except Exception as chyba:
        print(f"Stažení ID z mesta.obce.cz selhalo: {chyba}")
        raise SystemExit(1)
