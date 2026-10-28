"""Ruční stažení a aktualizace IČO městských částí v již připravené databázi."""

import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Dict

import psycopg2

import stahni_ico_mestske_casti

SOUBOR_ICO = Path(__file__).resolve().parent.parent / "tabulky" / "ico_mestske_casti.csv"


def aktualizovat_ico_mestskych_casti(
    cursor: psycopg2.extensions.cursor, soubor: Path
) -> Dict[str, int]:
    """Aktualizuje vazby z ověřeného CSV; transakci řídí volající."""
    with soubor.open(encoding="utf-8", newline="") as f:
        radky = list(csv.DictReader(f, delimiter=";"))
    if not radky:
        raise ValueError("Soubor IČO městských částí je prázdný.")

    # Po dobu porovnání a zápisu zabrání souběžným změnám identifikátorů.
    cursor.execute("LOCK TABLE ids IN SHARE ROW EXCLUSIVE MODE;")
    cursor.execute("""
        SELECT nazev, pk_id
        FROM geo_locations
        WHERE typ = 'MESTSKA_CAST';
    """)
    casti_mapa = {nazev: pk_id for nazev, pk_id in cursor.fetchall()}
    nezname_nazvy = {r["nazev"].strip() for r in radky} - casti_mapa.keys()
    if nezname_nazvy:
        raise ValueError(
            "Při aktualizaci IČO městských částí se ve zdrojovém souboru vyskytují neznámé názvy: "
            + ", ".join(sorted(nezname_nazvy))
        )

    cursor.execute("""
        SELECT i.location_pk, i.pk, i.value
        FROM ids i
        JOIN geo_locations mc ON mc.pk_id = i.location_pk
        WHERE i.type = 'ICO' AND mc.typ = 'MESTSKA_CAST';
    """)
    existujici_ico = {}
    for pk_casti, pk_ico, ico in cursor.fetchall():
        if pk_casti in existujici_ico:
            raise ValueError(f"Městská část {pk_casti} má více IČO; nejprve opravte její vazby.")
        existujici_ico[pk_casti] = (pk_ico, ico)

    pocty = {"vlozeno": 0, "zmeneno": 0, "beze_zmeny": 0, "mimo_zdroj": 0}
    casti_ve_zdroji = set()
    for row in radky:
        pk_casti = casti_mapa[row["nazev"].strip()]
        ico = row["ico"].strip()
        casti_ve_zdroji.add(pk_casti)
        puvodni = existujici_ico.get(pk_casti)
        if puvodni is None:
            cursor.execute(
                "INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, 'ICO', 80);",
                (pk_casti, ico),
            )
            pocty["vlozeno"] += 1
        elif puvodni[1] != ico:
            cursor.execute("UPDATE ids SET value = %s WHERE pk = %s;", (ico, puvodni[0]))
            pocty["zmeneno"] += 1
        else:
            pocty["beze_zmeny"] += 1

    # Zjistí, kolik městských částí má IČO v naší databázi, ale není v aktuálním zdroji.
    pocty["mimo_zdroj"] = len(existujici_ico.keys() - casti_ve_zdroji)
    return pocty


def main() -> None:
    print(f"{datetime.now().astimezone().isoformat(timespec='seconds')} — Zahajuji aktualizaci IČO městských částí.", flush=True)
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )
    try:
        with conn:
            with conn.cursor() as cursor:
                stahni_ico_mestske_casti.CILOVY_SOUBOR = str(SOUBOR_ICO)
                pocet = stahni_ico_mestske_casti.stahni_ico_mestske_casti()
                print(f"Staženo {pocet} městských částí.", flush=True)
                pocty = aktualizovat_ico_mestskych_casti(cursor, SOUBOR_ICO)
        print(f"{datetime.now().astimezone().isoformat(timespec='seconds')} — Aktualizace dokončena: {pocty}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Aktualizace IČO městských částí selhala: {chyba}", flush=True)
        raise SystemExit(1)
