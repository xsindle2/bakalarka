"""Sestavení územního základu z místních CSV do nového schématu."""

import csv
import json
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from main import vycistit_nazev

SLOZKA_TABULEK = Path(__file__).resolve().parent.parent / "tabulky"
CILOVE_SCHEMA = "data_pracovni"


def nacist_csv(
    soubor: Path, povinne: List[str], unikatni: List[str], extra_regio: bool = False,
    nepovinne_hodnoty: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """Načte používaná pole a ověří hodnoty i jednoznačnost kódů."""
    radky = []
    sloupce = povinne + (nepovinne_hodnoty or [])
    pouzite = {pole: set() for pole in unikatni}
    with soubor.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, strict=True)
        chybejici = set(sloupce) - set(reader.fieldnames or [])
        if chybejici:
            raise ValueError(f"{soubor.name}: chybějící sloupce: {', '.join(sorted(chybejici))}")
        for row in reader:
            if extra_regio and (row.get("text") or "").strip() == "Extra-Regio":
                continue
            hodnoty = {pole: (row.get(pole) or "").strip() for pole in sloupce}
            for pole in povinne:
                if not hodnoty[pole]:
                    raise ValueError(f"{soubor.name}, řádek {reader.line_num}: prázdné pole {pole}")
            for pole in unikatni:
                if not hodnoty[pole]:
                    continue
                if hodnoty[pole] in pouzite[pole]:
                    raise ValueError(f"{soubor.name}: duplicitní {pole}: {hodnoty[pole]}")
                pouzite[pole].add(hodnoty[pole])
            radky.append(hodnoty)
    if not radky:
        raise ValueError(f"{soubor.name}: žádné použitelné územní záznamy.")
    return radky


def vlozit_uzel(
    cursor: psycopg2.extensions.cursor, typ: str, nazev: str,
    rodic: Optional[Tuple[int, str]], identifikatory: List[Tuple[str, str, int]],
) -> Tuple[int, str]:
    """Vloží uzel, cestu a identifikátory do schématu nastaveného transakcí."""
    cursor.execute(
        "INSERT INTO geo_locations (parent_id, typ, nazev) VALUES (%s, %s, %s) RETURNING pk_id",
        (rodic[0] if rodic else None, typ, nazev),
    )
    pk = cursor.fetchone()[0]
    cesta = f"{rodic[1]}.{pk}" if rodic else str(pk)
    cursor.execute("UPDATE geo_locations SET ltree_path = %s WHERE pk_id = %s", (cesta, pk))
    for typ_id, hodnota, priorita in identifikatory:
        cursor.execute(
            "INSERT INTO ids (location_pk, type, value, priority) VALUES (%s, %s, %s, %s)",
            (pk, typ_id, hodnota, priorita),
        )
    return pk, cesta


def overit_strom(
    cursor: psycopg2.extensions.cursor, pocty: Dict[str, int], okresy_bez_ruian: int = 0
) -> None:
    """Ověří uložený výsledek v cílovém schématu před potvrzením transakce."""
    cursor.execute("SELECT typ, count(*) FROM geo_locations GROUP BY typ;")
    if dict(cursor.fetchall()) != pocty:
        raise ValueError("Počty vytvořených jednotek neodpovídají vstupům.")

    cursor.execute("""
        SELECT u.pk_id FROM geo_locations u
        LEFT JOIN geo_locations p ON p.pk_id = u.parent_id
        WHERE u.ltree_path IS NULL
           OR (u.typ = 'KRAJ' AND (u.parent_id IS NOT NULL OR u.ltree_path::text <> u.pk_id::text))
           OR (u.typ <> 'KRAJ' AND (
               p.pk_id IS NULL
               OR (u.typ = 'OKRES' AND p.typ <> 'KRAJ')
               OR (u.typ = 'OBEC' AND p.typ <> 'OKRES')
               OR (u.typ IN ('MESTSKA_CAST', 'CAST_OBCE') AND p.typ <> 'OBEC')
               OR u.ltree_path::text IS DISTINCT FROM p.ltree_path::text || '.' || u.pk_id::text
           ))
        LIMIT 1;
    """)
    chyba = cursor.fetchone()
    if chyba:
        raise ValueError(f"Nesprávný rodič nebo stromová cesta uzlu {chyba[0]}.")

    cursor.execute("""
        SELECT u.typ, i.type, count(*) FROM ids i
        LEFT JOIN geo_locations u ON u.pk_id = i.location_pk
        GROUP BY u.typ, i.type;
    """)
    skutecne = {(typ, typ_id): pocet for typ, typ_id, pocet in cursor.fetchall()}
    ocekavane = {}
    for typ, typy_id in (
        ("KRAJ", ("NUTS3", "RUIAN")), ("OKRES", ("LAU1", "RUIAN")),
        ("OBEC", ("LAU2",)), ("MESTSKA_CAST", ("MOMC",)), ("CAST_OBCE", ("KOD_COBCE",)),
    ):
        for typ_id in typy_id:
            ocekavane[(typ, typ_id)] = pocty[typ]
    ocekavane[("OKRES", "RUIAN")] -= okresy_bez_ruian
    ocekavane = {klic: pocet for klic, pocet in ocekavane.items() if pocet}
    if skutecne != ocekavane:
        raise ValueError("Počty územních identifikátorů neodpovídají uzlům.")


def sestavit_uzemni_zaklad(
    cursor: psycopg2.extensions.cursor, slozka: Path, schema: str
) -> Dict[str, object]:
    """Sestaví nové schéma; transakci a případný rollback řídí volající."""
    kraje = nacist_csv(slozka / "CIS0100_CS.csv",
        ["chodnota", "text", "cznuts", "kod_ruian"], ["chodnota", "cznuts", "kod_ruian"], extra_regio=True)
    okresy = nacist_csv(slozka / "CIS0101_CS.csv",
        ["chodnota", "text", "cznuts", "okres_lau"],
        ["chodnota", "cznuts", "okres_lau", "kod_ruian"], extra_regio=True, nepovinne_hodnoty=["kod_ruian"])
    okresy_bez_ruian = [
        {"kod_csu": row["chodnota"], "nazev": row["text"], "lau1": row["okres_lau"]}
        for row in okresy if not row["kod_ruian"]
    ]
    obce = nacist_csv(slozka / "VAZ0043_0101_CS.csv",
        ["chodnota1", "text1", "chodnota2"], ["chodnota1"])
    mestske_casti = nacist_csv(slozka / "VAZ0044_0043_CS.csv",
        ["chodnota1", "text1", "chodnota2"], ["chodnota1"])
    casti_obci = nacist_csv(slozka / "VAZ0042_0043_CS.csv",
        ["chodnota1", "text1", "chodnota2"], ["chodnota1"])

    cursor.execute(
        sql.SQL("CREATE SCHEMA {schema}").format(schema=sql.Identifier(schema))
    )
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;")
    cursor.execute("CREATE EXTENSION IF NOT EXISTS ltree WITH SCHEMA public;")
    # Nastavení platí jen do konce transakce. Dočasné tabulky nemají přednost.
    cursor.execute(
        sql.SQL("SET LOCAL search_path TO {schema}, public, pg_temp").format(
            schema=sql.Identifier(schema)
        )
    )
    cursor.execute("""
        CREATE TABLE geo_locations (
            pk_id SERIAL PRIMARY KEY,
            parent_id INTEGER REFERENCES geo_locations(pk_id) ON DELETE CASCADE,
            typ VARCHAR(50) NOT NULL, nazev VARCHAR(255) NOT NULL,
            ltree_path public.ltree
        );
        CREATE TABLE ids (
            pk SERIAL PRIMARY KEY,
            location_pk INTEGER REFERENCES geo_locations(pk_id) ON DELETE CASCADE,
            value VARCHAR(50) NOT NULL, type VARCHAR(20) NOT NULL,
            priority INTEGER DEFAULT 0
        );
    """)

    kraje_podle_nuts = {}
    for row in kraje:
        kraje_podle_nuts[row["cznuts"]] = vlozit_uzel(cursor, "KRAJ", row["text"], None,
            [("NUTS3", row["cznuts"], 80), ("RUIAN", row["kod_ruian"], 80)])
    okresy_podle_kodu = {}
    for row in okresy:
        rodice = [uzel for nuts, uzel in kraje_podle_nuts.items() if row["cznuts"].startswith(nuts)]
        if len(rodice) != 1:
            raise ValueError(f"Okres {row['chodnota']} nemá právě jeden kraj podle cznuts {row['cznuts']}.")
        identifikatory = [("LAU1", row["okres_lau"], 80)]
        if row["kod_ruian"]:
            identifikatory.append(("RUIAN", row["kod_ruian"], 80))
        okresy_podle_kodu[row["chodnota"]] = vlozit_uzel(cursor, "OKRES", row["text"], rodice[0], identifikatory)
    obce_podle_lau2 = {}
    for row in obce:
        rodic = okresy_podle_kodu.get(row["chodnota2"])
        if rodic is None:
            raise ValueError(f"Obec {row['chodnota1']}: chybí okres {row['chodnota2']}.")
        obce_podle_lau2[row["chodnota1"]] = vlozit_uzel(cursor, "OBEC", vycistit_nazev(row["text1"]), rodic,
            [("LAU2", row["chodnota1"], 100)])
    for radky, typ, typ_id, priorita in (
        (mestske_casti, "MESTSKA_CAST", "MOMC", 90), (casti_obci, "CAST_OBCE", "KOD_COBCE", 85),
    ):
        for row in radky:
            rodic = obce_podle_lau2.get(row["chodnota2"])
            if rodic is None:
                raise ValueError(f"{typ} {row['chodnota1']}: chybí obec {row['chodnota2']}.")
            vlozit_uzel(cursor, typ, row["text1"], rodic, [(typ_id, row["chodnota1"], priorita)])

    pocty = {"KRAJ": len(kraje), "OKRES": len(okresy), "OBEC": len(obce),
             "MESTSKA_CAST": len(mestske_casti), "CAST_OBCE": len(casti_obci)}
    overit_strom(cursor, pocty, len(okresy_bez_ruian))
    cursor.execute("CREATE INDEX idx_ids_value_btree ON ids (value)")
    cursor.execute("CREATE INDEX idx_ids_value_gin ON ids USING GIN (value public.gin_trgm_ops)")
    cursor.execute("CREATE INDEX path_gist_idx ON geo_locations USING GIST (ltree_path)")
    return {"schema": schema, "pocty": pocty, "okresy_bez_ruian": okresy_bez_ruian}


def main() -> None:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
    )
    try:
        with conn:
            with conn.cursor() as cursor:
                vysledek = sestavit_uzemni_zaklad(cursor, SLOZKA_TABULEK, CILOVE_SCHEMA)
        print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Sestavení územního základu selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
