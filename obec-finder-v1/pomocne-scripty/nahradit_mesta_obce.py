"""Nahrazení identifikátorů mesta.obce.cz z místního CSV v datovém schématu."""

import csv
import json
import os
from pathlib import Path
import re
import sys
from typing import Dict

import psycopg2
from psycopg2 import sql
from chyby_importu import ChybaZdrojovychDat

SOUBOR_MESTA_OBCE = Path(__file__).resolve().parent.parent / "tabulky" / "mesta_obce_id.csv"
CILOVE_SCHEMA = "data_pracovni"


def nahradit_mesta_obce(
    cursor: psycopg2.extensions.cursor, soubor: Path, schema: str
) -> Dict[str, object]:
    """Připraví párování a nahradí vazby zdroje; transakci řídí volající."""
    radky = []
    chyby = []
    pouzite_kody = {}
    with soubor.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", strict=True)
        if not {"zuj", "id_mesta_obce"}.issubset(reader.fieldnames or []):
            raise ChybaZdrojovychDat("Soubor musí obsahovat sloupce zuj a id_mesta_obce.")
        for row in reader:
            zuj = (row.get("zuj") or "").strip().lstrip("0")
            id_webu = (row.get("id_mesta_obce") or "").strip().lstrip("0")
            if not re.fullmatch(r"[0-9]{6}", zuj):
                chyby.append({"radek": reader.line_num, "sloupec": "zuj",
                              "hodnota": zuj, "duvod": "Neplatný kód ZÚJ; vynechán celý řádek."})
                continue
            if not id_webu:
                chyby.append({"radek": reader.line_num, "sloupec": "id_mesta_obce",
                              "hodnota": id_webu, "duvod": "Prázdné ID webu; vynechán celý řádek."})
                continue
            klic = zuj
            if klic in pouzite_kody:
                if pouzite_kody[klic] != id_webu:
                    raise ChybaZdrojovychDat(f"Více odlišných ID webu pro ZÚJ {zuj}.")
                continue
            pouzite_kody[klic] = id_webu
            radky.append({"zuj": zuj, "id_mesta_obce": id_webu})
    if not radky and not chyby:
        raise ChybaZdrojovychDat("Soubor mesta.obce.cz je prázdný.")

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
        SELECT i.value, o.pk_id, o.typ FROM ids i
        JOIN geo_locations o ON o.pk_id = i.location_pk
        WHERE (i.type = 'LAU2' AND o.typ = 'OBEC')
           OR (i.type = 'MOMC' AND o.typ = 'MESTSKA_CAST');
    """)
    uzly = {}
    for kod, pk, typ in cursor.fetchall():
        klic = kod.lstrip("0")
        if klic in uzly:
            raise ValueError(f"Nejednoznačný kód ZÚJ v cílovém schématu: {kod}")
        uzly[klic] = (pk, typ)

    vazby = []
    nesparovane = []
    for row in radky:
        uzel = uzly.get(row["zuj"])
        if uzel is None:
            nesparovane.append(row)
        else:
            pk, typ = uzel
            vazby.append((pk, row["id_mesta_obce"], typ))
    if not vazby:
        raise ChybaZdrojovychDat("Žádné ID mesta.obce.cz se nespárovalo. Původní vazby se nenahrazují.")

    cursor.execute("""
        DELETE FROM ids i USING geo_locations o
        WHERE i.location_pk = o.pk_id AND i.type = 'MESTA_OBCE'
          AND o.typ IN ('OBEC', 'MESTSKA_CAST');
    """)
    odstraneno = cursor.rowcount
    vlozeno = {"OBEC": 0, "MESTSKA_CAST": 0}
    for pk, id_webu, typ in vazby:
        cursor.execute("""
            INSERT INTO ids (location_pk, value, type, priority)
            VALUES (%s, %s, 'MESTA_OBCE', 40);
        """, (pk, id_webu))
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
                vysledek = nahradit_mesta_obce(cursor, SOUBOR_MESTA_OBCE, CILOVE_SCHEMA)
        print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Nahrazení mesta.obce.cz selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
