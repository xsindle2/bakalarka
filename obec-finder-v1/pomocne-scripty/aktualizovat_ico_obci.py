"""Ruční stažení a aktualizace IČO obcí v již připravené databázi."""

import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Dict

import psycopg2

import stahni_ico

SOUBOR_ICO = Path(__file__).resolve().parent.parent / "tabulky" / "ico_obce.csv"


def aktualizovat_ico_obci(
    cursor: psycopg2.extensions.cursor, soubor: Path
) -> Dict[str, int]:
    """Aktualizuje vazby z ověřeného CSV; transakci řídí volající."""
    with soubor.open(encoding="utf-8", newline="") as f:
        radky = list(csv.DictReader(f, delimiter=";"))
    if not radky:
        raise ValueError("Soubor IČO obcí je prázdný.")

    # Po dobu porovnání a zápisu zabrání souběžným změnám identifikátorů.
    cursor.execute("LOCK TABLE ids IN SHARE ROW EXCLUSIVE MODE;")
    cursor.execute("""
        SELECT i.value, i.location_pk
        FROM ids i
        JOIN geo_locations o ON o.pk_id = i.location_pk
        WHERE i.type = 'LAU2' AND o.typ = 'OBEC';
    """)
    obce_mapa = {kod: pk_id for kod, pk_id in cursor.fetchall()}
    nezname_kody = {r["kod_obce"].strip() for r in radky} - obce_mapa.keys()
    if nezname_kody:
        raise ValueError(
            "Při aktualizaci IČO obcí se v zdrojovém souboru vyskytují neznámé LAU2: "
            + ", ".join(sorted(nezname_kody))
        )

    cursor.execute("""
        SELECT i.location_pk, i.pk, i.value
        FROM ids i
        JOIN geo_locations o ON o.pk_id = i.location_pk
        WHERE i.type = 'ICO' AND o.typ = 'OBEC';
    """)
    existujici_ico = {}
    for pk_obce, pk_ico, ico in cursor.fetchall():
        if pk_obce in existujici_ico:
            raise ValueError(f"Obec {pk_obce} má více IČO; nejprve opravte její vazby.")
        existujici_ico[pk_obce] = (pk_ico, ico)

    pocty = {"vlozeno": 0, "zmeneno": 0, "beze_zmeny": 0, "mimo_zdroj": 0}
    obce_ve_zdroji = set()
    for row in radky:
        pk_obce = obce_mapa[row["kod_obce"].strip()]
        ico = row["ico"].strip()
        obce_ve_zdroji.add(pk_obce)
        puvodni = existujici_ico.get(pk_obce)
        if puvodni is None:
            cursor.execute(
                "INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, 'ICO', 80);",
                (pk_obce, ico),
            )
            pocty["vlozeno"] += 1
        elif puvodni[1] != ico:
            cursor.execute("UPDATE ids SET value = %s WHERE pk = %s;", (ico, puvodni[0]))
            pocty["zmeneno"] += 1
        else:
            pocty["beze_zmeny"] += 1

    # Zjistí, kolik obcí má IČO v nasi databázi, ale není v aktuálním zdroji.
    pocty["mimo_zdroj"] = len(existujici_ico.keys() - obce_ve_zdroji)
    return pocty


def main() -> None:
    print(f"{datetime.now().astimezone().isoformat(timespec='seconds')} — Zahajuji aktualizaci IČO obcí.", flush=True)
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )
    try:
        with conn:
            with conn.cursor() as cursor:
                stahni_ico.CILOVY_SOUBOR = str(SOUBOR_ICO)
                pocet = stahni_ico.stahni_ico()
                print(f"Staženo {pocet} obcí.", flush=True)
                pocty = aktualizovat_ico_obci(cursor, SOUBOR_ICO)
        print(f"{datetime.now().astimezone().isoformat(timespec='seconds')} — Aktualizace dokončena: {pocty}", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Aktualizace IČO obcí selhala: {chyba}", flush=True)
        raise SystemExit(1)
