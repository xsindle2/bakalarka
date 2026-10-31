"""Nahrazení PSČ obcí, městských částí a částí obcí z místního CSV."""

import csv
import json
import os
from pathlib import Path
import re
import sys
from typing import Dict

import psycopg2
from psycopg2 import sql

SOUBOR_PSC = Path(__file__).resolve().parent.parent / "tabulky" / "zv_cobce_psc.csv"
CILOVE_SCHEMA = "data_pracovni"


def nahradit_psc(
    cursor: psycopg2.extensions.cursor, soubor: Path, schema: str
) -> Dict[str, object]:
    """Připraví vazby PSČ a nahradí je; transakci řídí volající."""
    typy = (
        ("kodobce", "LAU2", "OBEC", 50),
        ("kodmomc", "MOMC", "MESTSKA_CAST", 60),
        ("kodcobce", "KOD_COBCE", "CAST_OBCE", 60),
    )
    radky = []
    chyby = []
    with soubor.open(encoding="windows-1250", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", strict=True)
        if not {"psc", "kodobce", "kodmomc", "kodcobce"}.issubset(reader.fieldnames or []):
            raise ValueError("Soubor musí obsahovat sloupce psc, kodobce, kodmomc a kodcobce.")
        for row in reader:
            psc = (row.get("psc") or "").replace(" ", "").strip()
            if not re.fullmatch(r"[0-9]{5}", psc):
                chyby.append({"radek": reader.line_num, "sloupec": "psc",
                              "hodnota": psc, "duvod": "Neplatné PSČ; vynechán celý řádek."})
                continue
            zaznam = {"radek": reader.line_num, "psc": psc}
            for sloupec, _, _, _ in typy:
                kod = (row.get(sloupec) or "").strip()
                if not kod and sloupec != "kodobce":
                    zaznam[sloupec] = ""
                elif not re.fullmatch(r"[0-9]{1,6}", kod) or not kod.lstrip("0"):
                    chyby.append({"radek": reader.line_num, "sloupec": sloupec,
                                  "hodnota": kod, "duvod": "Neplatný územní kód; vazba vynechána."})
                    zaznam[sloupec] = ""
                else:
                    zaznam[sloupec] = kod
            radky.append(zaznam)
    if not radky and not chyby:
        raise ValueError("Soubor PSČ je prázdný.")

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
    mapy = {}
    for sloupec, identifikator, typ, _ in typy:
        cursor.execute("""
            SELECT i.value, o.pk_id FROM ids i
            JOIN geo_locations o ON o.pk_id = i.location_pk
            WHERE i.type = %s AND o.typ = %s;
        """, (identifikator, typ))
        mapa = {}
        for kod, pk in cursor.fetchall():
            klic = kod.lstrip("0")
            if klic in mapa:
                raise ValueError(f"Nejednoznačný {identifikator} v cílovém schématu: {kod}")
            mapa[klic] = pk
        mapy[sloupec] = mapa

    vazby = []
    pouzite_vazby = set()
    nesparovane = []
    for row in radky:
        for sloupec, _, typ, priorita in typy:
            kod = row[sloupec]
            if not kod:
                continue
            pk = mapy[sloupec].get(kod.lstrip("0"))
            if pk is None:
                nesparovane.append({"radek": row["radek"], "sloupec": sloupec,
                                    "hodnota": kod, "psc": row["psc"]})
                continue
            klic = (pk, row["psc"])
            if klic not in pouzite_vazby:
                pouzite_vazby.add(klic)
                vazby.append((pk, row["psc"], priorita, typ))
    if not vazby:
        raise ValueError("Žádná platná vazba PSČ se nespárovala. Původní vazby se nenahrazují.")

    cursor.execute("""
        DELETE FROM ids i USING geo_locations o
        WHERE i.location_pk = o.pk_id AND i.type = 'PSC'
          AND o.typ IN ('OBEC', 'MESTSKA_CAST', 'CAST_OBCE');
    """)
    odstraneno = cursor.rowcount
    vlozeno = {typ: 0 for _, _, typ, _ in typy}
    for pk, psc, priorita, typ in vazby:
        cursor.execute("""
            INSERT INTO ids (location_pk, value, type, priority)
            VALUES (%s, %s, 'PSC', %s);
        """, (pk, psc, priorita))
        vlozeno[typ] += 1
    return {
        "schema": schema, "stav": "castecny" if nesparovane or chyby else "uplny",
        "odstraneno": odstraneno, "vlozeno": vlozeno,
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
                vysledek = nahradit_psc(cursor, SOUBOR_PSC, CILOVE_SCHEMA)
        print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Nahrazení PSČ selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
