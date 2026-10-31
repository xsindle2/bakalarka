"""Nahrazení Q-kódů a GeoNames ID obcí z místního CSV v datovém schématu."""

import csv
import json
import os
from pathlib import Path
import re
import sys
from typing import Dict

import psycopg2
from psycopg2 import sql

SOUBOR_WIKIDATA = Path(__file__).resolve().parent.parent / "tabulky" / "wikidata_obce.csv"
CILOVE_SCHEMA = "data_pracovni"


def nahradit_wikidata_obci(
    cursor: psycopg2.extensions.cursor, soubor: Path, schema: str
) -> Dict[str, object]:
    """Připraví párování a nahradí vazby zdroje; transakci řídí volající."""
    radky = []
    chyby = []
    with soubor.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, strict=True)
        if not {"qcode", "lau2", "geonames"}.issubset(reader.fieldnames or []):
            raise ValueError("Soubor musí obsahovat sloupce qcode, lau2 a geonames.")
        for row in reader:
            qcode = (row.get("qcode") or "").strip()
            lau2 = (row.get("lau2") or "").strip()
            geonames = (row.get("geonames") or "").strip()
            if not re.fullmatch(r"[0-9]{6}", lau2) or lau2 == "000000":
                chyby.append({"radek": reader.line_num, "sloupec": "lau2",
                              "hodnota": lau2, "duvod": "Neplatné LAU2; vynechán celý řádek."})
                continue
            if not re.fullmatch(r"Q[1-9][0-9]*", qcode):
                chyby.append({"radek": reader.line_num, "sloupec": "qcode",
                              "hodnota": qcode, "duvod": "Neplatný Q-kód; vazba vynechána."})
                qcode = ""
            if geonames and not re.fullmatch(r"[0-9]+", geonames):
                chyby.append({"radek": reader.line_num, "sloupec": "geonames",
                              "hodnota": geonames, "duvod": "Neplatné GeoNames ID; vazba vynechána."})
                geonames = ""
            if qcode or geonames:
                radky.append({"qcode": qcode, "lau2": lau2, "geonames": geonames})
    if not radky and not chyby:
        raise ValueError("Soubor Wikidat je prázdný.")

    # Chybné jméno schématu nesmí vést k použití tabulek z public.
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
    pouzite_vazby = set()
    sparovane_obce = set()
    nesparovane = []
    for row in radky:
        pk = obce.get(row["lau2"])
        if pk is None:
            nesparovane.append(row)
            continue
        sparovane_obce.add(pk)
        for typ, hodnota, priorita in (
            ("QCODE", row["qcode"], 90), ("GEONAMES", row["geonames"], 70),
        ):
            klic = (pk, typ, hodnota)
            if hodnota and klic not in pouzite_vazby:
                pouzite_vazby.add(klic)
                vazby.append((pk, hodnota, typ, priorita))
    if not vazby:
        raise ValueError("Žádná platná vazba Wikidat a GeoNames id se nespárovala s obcí.")

    cursor.execute("""
        DELETE FROM ids i USING geo_locations o
        WHERE i.location_pk = o.pk_id AND i.type IN ('QCODE', 'GEONAMES')
          AND o.typ = 'OBEC';
    """)
    odstraneno = cursor.rowcount
    vlozeno = {"QCODE": 0, "GEONAMES": 0}
    for pk, hodnota, typ, priorita in vazby:
        cursor.execute("""
            INSERT INTO ids (location_pk, value, type, priority)
            VALUES (%s, %s, %s, %s);
        """, (pk, hodnota, typ, priorita))
        vlozeno[typ] += 1
    return {
        "schema": schema, "stav": "castecny" if nesparovane or chyby else "uplny",
        "odstraneno": odstraneno, "vlozeno": vlozeno,
        "sparovane_obce": len(sparovane_obce),
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
                vysledek = nahradit_wikidata_obci(cursor, SOUBOR_WIKIDATA, CILOVE_SCHEMA)
        print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Nahrazení Wikidat obcí selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
