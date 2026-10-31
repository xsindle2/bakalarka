"""Nahrazení IČO pražských městských částí v existujícím datovém schématu z místního CSV."""

import csv
import json
import os
from pathlib import Path
import re
import sys
from typing import Dict

import psycopg2
from psycopg2 import sql

SOUBOR_ICO = Path(__file__).resolve().parent.parent / "tabulky" / "ico_mestske_casti.csv"
CILOVE_SCHEMA = "data_pracovni"


def nahradit_ico_mestskych_casti(
    cursor: psycopg2.extensions.cursor, soubor: Path, schema: str
) -> Dict[str, object]:
    """Připraví párování a nahradí IČO pražských částí; transakci řídí volající."""
    radky = []
    chyby = []
    pouzita_ico = set()
    pouzite_nazvy = set()
    with soubor.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", strict=True)
        if not {"ico", "nazev"}.issubset(reader.fieldnames or []):
            raise ValueError("Soubor musí obsahovat sloupce ico a nazev.")
        for row in reader:
            ico = (row.get("ico") or "").strip()
            nazev = (row.get("nazev") or "").strip()
            if not re.fullmatch(r"[0-9]{8}", ico):
                chyby.append({"radek": reader.line_num, "sloupec": "ico",
                              "hodnota": ico, "duvod": "Neplatné IČO; vynechán celý řádek."})
                continue
            if not nazev:
                chyby.append({"radek": reader.line_num, "sloupec": "nazev",
                              "hodnota": nazev, "duvod": "Prázdný název městské části; vynechán celý řádek."})
                continue
            if ico in pouzita_ico or nazev in pouzite_nazvy:
                raise ValueError(f"Duplicitní IČO nebo název městské části: {ico}, {nazev}.")
            pouzita_ico.add(ico)
            pouzite_nazvy.add(nazev)
            radky.append({"ico": ico, "nazev": nazev})
    if not radky and not chyby:
        raise ValueError("Soubor IČO pražských městských částí je prázdný.")

    # Chybné jméno schématu nesmí vést k použití tabulek z public
    cursor.execute("""
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname = %s AND tablename IN ('geo_locations', 'ids');
    """, (schema,))
    if {row[0] for row in cursor.fetchall()} != {"geo_locations", "ids"}:
        raise ValueError(f"Cílové schéma {schema!r} musí obsahovat geo_locations a ids.")
    cursor.execute(
        sql.SQL("SET LOCAL search_path TO {schema}, public, pg_temp").format(
            schema=sql.Identifier(schema)
        )
    )
    cursor.execute("LOCK TABLE geo_locations, ids IN SHARE ROW EXCLUSIVE MODE;")
    cursor.execute("""
        SELECT nazev, pk_id FROM geo_locations
        WHERE typ = 'MESTSKA_CAST';
    """)
    casti = {}
    for nazev, pk in cursor.fetchall():
        if nazev in casti:
            raise ValueError(f"Nejednoznačný název městské části: {nazev}")
        casti[nazev] = pk

    vazby = []
    nesparovane = []
    for row in radky:
        pk = casti.get(row["nazev"])
        if pk is None:
            nesparovane.append(row)
        else:
            vazby.append((pk, row["ico"]))
    if not vazby:
        raise ValueError("Žádné IČO nelze přiřadit k pražské městské části. Původní vazby se nenahrazují.")

    cursor.execute("""
        DELETE FROM ids i USING geo_locations o
        WHERE i.location_pk = o.pk_id AND i.type = 'ICO'
          AND o.typ = 'MESTSKA_CAST';
    """)
    odstraneno = cursor.rowcount
    for pk, ico in vazby:
        cursor.execute("""
            INSERT INTO ids (location_pk, value, type, priority)
            VALUES (%s, %s, 'ICO', 80);
        """, (pk, ico))
    return {
        "schema": schema, "stav": "castecny" if nesparovane or chyby else "uplny",
        "odstraneno": odstraneno, "vlozeno": len(vazby),
        "nesparovano": len(nesparovane), "nesparovane": nesparovane,
        "chybnych_hodnot": len(chyby), "chyby": chyby,
    }


def main() -> None:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
    )
    try:
        with conn:
            with conn.cursor() as cursor:
                vysledek = nahradit_ico_mestskych_casti(cursor, SOUBOR_ICO, CILOVE_SCHEMA)
        print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Nahrazení IČO pražských městských částí selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
