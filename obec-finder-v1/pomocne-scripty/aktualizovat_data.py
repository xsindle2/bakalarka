"""Stažení zdrojů a aktualizace doplňků nebo sestavení nové datové verze."""

import csv
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import sys
from typing import Dict
from zoneinfo import ZoneInfo
from zipfile import BadZipFile

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from aktivni_data import nacist_aktivni_schema, nastavit_aktivni_schema
from chyby_importu import ChybaZdrojovychDat
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


def nahradit_csv(nazev: str) -> None:
    """Nahradí CSV po potvrzení importu; chyba kopie již nevrací databázi."""
    try:
        shutil.copyfile(SLOZKA_NOVE / nazev, SLOZKA_PUVODNI / nazev)
    except OSError as chyba:
        raise RuntimeError(
            f"DB je aktualizovaná, ale kopírování {nazev} selhalo: {chyba}"
        ) from chyba


def aktualizovat_data() -> Dict[str, object]:
    """Aktualizuje nezávislé doplňky nebo transakčně sestaví celou novou verzi."""
    print("Stahuji územní data ČSÚ.", flush=True)
    stahni_csu()
    porovnani = porovnat_tabulky(SLOZKA_PUVODNI, SLOZKA_NOVE, SOUBORY_CSU)
    if porovnani["chybejici_puvodni"] or porovnani["chybejici_nove"]:
        raise ValueError(
            f"Chybějící původní CSV: {porovnani['chybejici_puvodni']}; "
            f"chybějící nové CSV: {porovnani['chybejici_nove']}."
        )
    novy_strom = bool(porovnani["zmenene"])
    zdroje = (
        ("ico_obce.csv", stahni_ico, nahradit_ico_obci),
        ("ico_mestske_casti.csv", stahni_ico_mestske_casti, nahradit_ico_mestskych_casti),
        ("wikidata_obce.csv", stahni_q_kody, nahradit_wikidata_obci),
        ("zv_cobce_psc.csv", stahni_psc, nahradit_psc),
        ("mesta_obce_id.csv", stahni_ids, nahradit_mesta_obce),
    )
    chyby_zdroju = {}
    for nazev, stahnout, _ in zdroje:
        faze = "stahovani"
        try:
            print(f"Stahuji {nazev}.", flush=True)
            stahnout()
            faze = "porovnani"
            cast = porovnat_tabulky(SLOZKA_PUVODNI, SLOZKA_NOVE, (nazev,))
            for klic in porovnani:
                porovnani[klic].extend(cast[klic])
            if cast["chybejici_puvodni"] or cast["chybejici_nove"]:
                raise ValueError(f"Chybějící původní nebo nové CSV: {nazev}.")
        except (OSError, ValueError, RuntimeError, KeyError, csv.Error, BadZipFile) as chyba:
            chyby_zdroju[nazev] = {"faze": faze, "chyba": str(chyba)}

    vysledek = porovnani.copy()
    vysledek["stav"] = "castecny" if chyby_zdroju else "beze_zmen"
    vysledek["cilove_schema"] = None
    importy = {}
    vysledek["importy"] = importy
    vysledek["chyby_zdroju"] = chyby_zdroju
    pouzite_puvodni_csv = []
    vysledek["pouzite_puvodni_csv"] = pouzite_puvodni_csv
    vysledek["predchozi_schema"] = None
    vysledek["uzemni_zaklad"] = None

    zmenene = porovnani["zmenene"]
    if not zmenene:
        return vysledek

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
    )
    try:
        with conn:
            with conn.cursor() as cursor:
                aktivni_schema = nacist_aktivni_schema(cursor)
                if novy_strom:
                    vysledek["predchozi_schema"] = aktivni_schema
                    cilove_schema = datetime.now(ZoneInfo("Europe/Prague")).strftime("data_%Y%m%d_%H%M%S")
                    vysledek["uzemni_zaklad"] = sestavit_uzemni_zaklad(cursor, SLOZKA_NOVE, cilove_schema)
                    for nazev, _, nahradit in zdroje:
                        if nazev not in chyby_zdroju or chyby_zdroju[nazev]["faze"] != "stahovani":
                            cursor.execute("SAVEPOINT import_doplnku;")
                            try:
                                report = nahradit(cursor, SLOZKA_NOVE / nazev, cilove_schema)
                            except (ChybaZdrojovychDat, OSError, UnicodeError, csv.Error,
                                    psycopg2.DataError, psycopg2.IntegrityError) as chyba:
                                cursor.execute("ROLLBACK TO SAVEPOINT import_doplnku;")
                                chyby_zdroju[nazev] = {"faze": "import", "chyba": str(chyba)}
                            else:
                                cursor.execute("RELEASE SAVEPOINT import_doplnku;")
                                importy[nazev] = report
                                continue
                            cursor.execute("RELEASE SAVEPOINT import_doplnku;")

                        try:
                            importy[nazev] = nahradit(cursor, SLOZKA_PUVODNI / nazev, cilove_schema)
                        except (ChybaZdrojovychDat, OSError, UnicodeError, csv.Error,
                                psycopg2.DataError, psycopg2.IntegrityError) as chyba:
                            raise ChybaZdrojovychDat(
                                f"Nelze načíst ani původní CSV {nazev}: {chyba}"
                            ) from chyba
                        pouzite_puvodni_csv.append(nazev)
                    nastavit_aktivni_schema(cursor, cilove_schema)
                else:
                    cilove_schema = aktivni_schema

        if novy_strom:
            nove_doplnky = [nazev for nazev in importy if nazev not in pouzite_puvodni_csv]
            for nazev in list(SOUBORY_CSU) + nove_doplnky:
                nahradit_csv(nazev)
        else:
            for nazev, _, nahradit in zdroje:
                if nazev not in zmenene or nazev in chyby_zdroju:
                    continue
                try:
                    with conn:
                        with conn.cursor() as cursor:
                            report = nahradit(cursor, SLOZKA_NOVE / nazev, cilove_schema)
                except (ChybaZdrojovychDat, OSError, UnicodeError, csv.Error,
                        psycopg2.DataError, psycopg2.IntegrityError) as chyba:
                    # Výjimka opustila with conn: změny tohoto zdroje už jsou vrácené.
                    chyby_zdroju[nazev] = {"faze": "import", "chyba": str(chyba)}
                    continue
                importy[nazev] = report
                nahradit_csv(nazev)
    finally:
        conn.close()

    vysledek["cilove_schema"] = cilove_schema
    if chyby_zdroju or any(r["stav"] == "castecny" for r in importy.values()):
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
            print("Doplňkové údaje jsou částečné. Podrobnosti jsou v repoortech importů a chyby_zdroju.")
        if vysledek["pouzite_puvodni_csv"]:
            print("Použita původní CSV: " + ", ".join(vysledek["pouzite_puvodni_csv"]))
    elif vysledek["stav"] == "castecny":
        print("Aktualizace doplňků je částečná. Podrobnosti jsou v importech a chyby_zdroju.")
    else:
        print("Změněné doplňkové údaje byly aktualizovány v aktivním schématu.")


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Aktualizace dat selhala: {chyba}", file=sys.stderr)
        raise SystemExit(1)
