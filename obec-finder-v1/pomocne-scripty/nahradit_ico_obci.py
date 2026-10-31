"""Nahrazení IČO obcí v existujícím datovém schématu z místního CSV."""

import csv
import json
import os
from pathlib import Path
import re
import sys
from typing import Dict

import psycopg2
from psycopg2 import sql

SOUBOR_ICO = Path(__file__).resolve().parent.parent / "tabulky" / "ico_obce.csv"
CILOVE_SCHEMA = "data_pracovni"


def nahradit_ico_obci(
    cursor: psycopg2.extensions.cursor, soubor: Path, schema: str
) -> Dict[str, object]:
    """Připraví párování a nahradí obecní IČO; transakci řídí volající."""
    radky = []
    chyby = []
    pouzita_ico = set()
    pouzite_kody = set()
    with soubor.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", strict=True)
        if not {"ico", "kod_obce"}.issubset(reader.fieldnames or []):
            raise ValueError("Soubor musí obsahovat sloupce ico a kod_obce.")
        for row in reader:
            ico = (row.get("ico") or "").strip()
            kod_obce = (row.get("kod_obce") or "").strip()
            if not re.fullmatch(r"[0-9]{8}", ico):
                chyby.append({"radek": reader.line_num, "sloupec": "ico",
                              "hodnota": ico, "duvod": "Neplatné IČO; vynechán celý řádek."})
                continue
            if not re.fullmatch(r"[0-9]{6}", kod_obce) or kod_obce == "000000":
                chyby.append({"radek": reader.line_num, "sloupec": "kod_obce",
                              "hodnota": kod_obce, "duvod": "Neplatné LAU2; vynechán celý řádek."})
                continue
            if ico in pouzita_ico or kod_obce in pouzite_kody:
                raise ValueError(f"Duplicitní IČO nebo kód obce: {ico}, {kod_obce}.")
            pouzita_ico.add(ico)
            pouzite_kody.add(kod_obce)
            radky.append({"ico": ico, "kod_obce": kod_obce})
    if not radky and not chyby:
        raise ValueError("Soubor IČO obcí je prázdný.")

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
        SELECT i.value, o.pk_id FROM ids i
        JOIN geo_locations o ON o.pk_id = i.location_pk
        WHERE i.type = 'LAU2' AND o.typ = 'OBEC';
    """)
    obce = {}
    for lau2, pk in cursor.fetchall():
        if lau2 in obce:
            raise ValueError(f"Nejednoznačné LAU2 v cílovém schématu: {lau2}")
        obce[lau2] = pk

    vazby = []
    nesparovane = []
    for row in radky:
        pk = obce.get(row["kod_obce"])
        if pk is None:
            nesparovane.append(row)
        else:
            vazby.append((pk, row["ico"]))
    if not vazby:
        raise ValueError("Žádné IČO nelze přiřadit k obci. Původní vazby se nenahrazují.")

    cursor.execute("""
        DELETE FROM ids i USING geo_locations o
        WHERE i.location_pk = o.pk_id AND i.type = 'ICO' AND o.typ = 'OBEC';
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
                vysledek = nahradit_ico_obci(cursor, SOUBOR_ICO, CILOVE_SCHEMA)
        print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Nahrazení IČO obcí selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
