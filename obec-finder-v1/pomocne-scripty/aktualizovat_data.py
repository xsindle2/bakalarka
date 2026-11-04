"""Stažení zdrojů a aktualizace doplňků nebo sestavení nové datové verze."""

from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import sys
from typing import Dict
from zoneinfo import ZoneInfo

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from aktivni_data import nacist_aktivni_schema, nastavit_aktivni_schema
from sestavit_uzemni_zaklad import sestavit_uzemni_zaklad
from nahradit_ico_obci import nahradit_ico_obci
from nahradit_ico_mestskych_casti import nahradit_ico_mestskych_casti
from nahradit_wikidata_obci import nahradit_wikidata_obci
from nahradit_psc import nahradit_psc
from nahradit_mesta_obce import nahradit_mesta_obce
from porovnat_tabulky import porovnat_tabulky
from stahni_csu import stahni_csu
from stahni_ico import stahni_ico
from stahni_ico_mestske_casti import stahni_ico_mestske_casti
from stahni_wikidata import stahni_q_kody
from stahni_psc import stahni_psc
from stahni_mesta_obce import stahni_ids

SLOZKA_PUVODNI = Path(__file__).resolve().parent.parent / "tabulky"
SLOZKA_NOVE = Path(__file__).resolve().parent.parent / "tabulky_nove"
SOUBORY_CSU = (
    "CIS0100_CS.csv", "CIS0101_CS.csv", "VAZ0043_0101_CS.csv",
    "VAZ0044_0043_CS.csv", "VAZ0042_0043_CS.csv",
)


def aktualizovat_data() -> Dict[str, object]:
    """Aktualizuje doplňky nebo vytvoří a aktivuje novou verzi; pak převezme CSV."""
    print("Stahuji územní data ČSÚ.", flush=True)
    stahni_csu()
    print("Stahuji IČO obcí.", flush=True)
    stahni_ico()
    print("Stahuji IČO městských částí.", flush=True)
    stahni_ico_mestske_casti()
    print("Stahuji Wikidata.", flush=True)
    stahni_q_kody()
    print("Stahuji PSČ.", flush=True)
    stahni_psc()
    print("Stahuji ID mesta.obce.cz.", flush=True)
    stahni_ids()

    porovnani = porovnat_tabulky(SLOZKA_PUVODNI, SLOZKA_NOVE)
    if porovnani["chybejici_puvodni"] or porovnani["chybejici_nove"]:
        raise ValueError(
            f"Chybějící původní CSV: {porovnani['chybejici_puvodni']}; "
            f"chybějící nové CSV: {porovnani['chybejici_nove']}."
        )
    vysledek = porovnani.copy()
    vysledek["stav"] = "beze_zmen"
    vysledek["cilove_schema"] = None
    vysledek["importy"] = {}
    vysledek["predchozi_schema"] = None
    vysledek["uzemni_zaklad"] = None

    zmenene = porovnani["zmenene"]
    if not zmenene:
        return vysledek
    novy_strom = any(soubor in zmenene for soubor in SOUBORY_CSU)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
    )
    importy = {}
    try:
        with conn:
            with conn.cursor() as cursor:
                aktivni_schema = nacist_aktivni_schema(cursor)
                if novy_strom:
                    vysledek["predchozi_schema"] = aktivni_schema
                    cilove_schema = datetime.now(ZoneInfo("Europe/Prague")).strftime("data_%Y%m%d_%H%M%S")
                    vysledek["uzemni_zaklad"] = sestavit_uzemni_zaklad(cursor, SLOZKA_NOVE, cilove_schema)
                else:
                    cilove_schema = aktivni_schema
                if novy_strom or "ico_obce.csv" in zmenene:
                    importy["ico_obce.csv"] = nahradit_ico_obci(cursor, SLOZKA_NOVE / "ico_obce.csv", cilove_schema)
                if novy_strom or "ico_mestske_casti.csv" in zmenene:
                    importy["ico_mestske_casti.csv"] = nahradit_ico_mestskych_casti(
                        cursor, SLOZKA_NOVE / "ico_mestske_casti.csv", cilove_schema)
                if novy_strom or "wikidata_obce.csv" in zmenene:
                    importy["wikidata_obce.csv"] = nahradit_wikidata_obci(cursor, SLOZKA_NOVE / "wikidata_obce.csv", cilove_schema)
                if novy_strom or "zv_cobce_psc.csv" in zmenene:
                    importy["zv_cobce_psc.csv"] = nahradit_psc(cursor, SLOZKA_NOVE / "zv_cobce_psc.csv", cilove_schema)
                if novy_strom or "mesta_obce_id.csv" in zmenene:
                    importy["mesta_obce_id.csv"] = nahradit_mesta_obce(cursor, SLOZKA_NOVE / "mesta_obce_id.csv", cilove_schema)
                if novy_strom:
                    nastavit_aktivni_schema(cursor, cilove_schema)
    finally:
        conn.close()

    prevzit = list(SOUBORY_CSU) + list(importy) if novy_strom else list(importy)
    for nazev in prevzit:
        try:
            shutil.copyfile(SLOZKA_NOVE / nazev, SLOZKA_PUVODNI / nazev)
        except OSError as chyba:
            raise RuntimeError(
                f"DB je aktualizovaná, ale kopírování {nazev} selhalo: {chyba}"
            ) from chyba

    vysledek["cilove_schema"] = cilove_schema
    vysledek["importy"] = importy
    
    if any(r["stav"] == "castecny" for r in importy.values()):
        vysledek["stav"] = "castecny" 
    else:
        vysledek["stav"] = "aktualizovano"
    return vysledek


def main() -> None:
    vysledek = aktualizovat_data()
    print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    if vysledek["stav"] == "beze_zmen":
        print("Používaná data se nezměnila. Aktualizace databáze není potřeba.")
    elif vysledek["uzemni_zaklad"] is not None:
        print(f"Nová datová verze {vysledek['cilove_schema']} byla sestavena a aktivována.")
        if vysledek["stav"] == "castecny":
            print("Doplňkové údaje jsou částečné. Podrobnosti jsou v reportech importů.")
    elif vysledek["stav"] == "castecny":
        print("Doplňkové údaje byly aktualizovány částečně. Podrobnosti jsou v reportech importů.")
    else:
        print("Změněné doplňkové údaje byly aktualizovány v aktivním schématu.")


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Aktualizace dat selhala: {chyba}", file=sys.stderr)
        raise SystemExit(1)
