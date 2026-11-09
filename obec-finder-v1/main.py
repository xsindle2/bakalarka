import time
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql
from aktivni_data import (
    nacist_aktivni_schema, nastavit_aktivni_schema, vytvorit_tabulku_aktivnich_dat,
)
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, List, Optional, Tuple
from validace_identifikatoru import (
    TypIdentifikatoru, VysledekValidace,
    overit_identifikator,
)

sys.path.insert(0, str(Path(__file__).resolve().parent / "pomocne-scripty"))
from sestavit_uzemni_zaklad import sestavit_uzemni_zaklad
from nahradit_ico_obci import nahradit_ico_obci
from nahradit_ico_mestskych_casti import nahradit_ico_mestskych_casti
from nahradit_wikidata_obci import nahradit_wikidata_obci
from nahradit_psc import nahradit_psc
from nahradit_mesta_obce import nahradit_mesta_obce

SLOZKA_TABULEK = Path(__file__).resolve().parent / "tabulky"

app = FastAPI()

# --- PYDANTIC MODELY ---
class Identifikator(BaseModel):
    type: str
    value: str
    priority: int = 80

class LocationCreate(BaseModel):
    nazev: str
    typ: str = "OBEC"      
    parent_kod: str        
    identifikatory: List[Identifikator] = []



def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )


# -------------------------------------------
@app.on_event("startup")
def startup_db() -> None:
    conn = None
    for _ in range(10):
        try:
            conn = get_db_connection()
            break
        except psycopg2.OperationalError:
            time.sleep(2)

    if not conn: return
    try:
        with conn:
            with conn.cursor() as cursor:
                vytvorit_tabulku_aktivnich_dat(cursor)
                cursor.execute("SELECT 1 FROM public.aktivni_data WHERE id = 1;")
                if cursor.fetchone() is not None:
                    return

                schema = datetime.now(ZoneInfo("Europe/Prague")).strftime("data_%Y%m%d_%H%M%S")
                uzemni_zaklad = sestavit_uzemni_zaklad(cursor, SLOZKA_TABULEK, schema)
                importy = {}
                importy["ico_obce.csv"] = nahradit_ico_obci(cursor, SLOZKA_TABULEK / "ico_obce.csv", schema)
                importy["ico_mestske_casti.csv"] = nahradit_ico_mestskych_casti(
                    cursor, SLOZKA_TABULEK / "ico_mestske_casti.csv", schema)
                importy["wikidata_obce.csv"] = nahradit_wikidata_obci(cursor, SLOZKA_TABULEK / "wikidata_obce.csv", schema)
                importy["zv_cobce_psc.csv"] = nahradit_psc(cursor, SLOZKA_TABULEK / "zv_cobce_psc.csv", schema)
                importy["mesta_obce_id.csv"] = nahradit_mesta_obce(cursor, SLOZKA_TABULEK / "mesta_obce_id.csv", schema)
                nastavit_aktivni_schema(cursor, schema)
        print(json.dumps({"uzemni_zaklad": uzemni_zaklad, "importy": importy}, ensure_ascii=False))
    finally:
        conn.close()


# --- ENDPOINTY PRO PŘIDÁNÍ A SMAZÁNÍ LOKACE ---

@app.post("/location", status_code=201)
def create_location(location: LocationCreate):
    """Vytvoří novou lokaci na základě známého kódu rodiče."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Najde interní ID rodiče na základě jeho známého kódu (parent_kod)
        cursor.execute("""
            SELECT gl.pk_id, gl.ltree_path 
            FROM ids i
            JOIN geo_locations gl ON i.location_pk = gl.pk_id
            WHERE i.value = %s
            LIMIT 1;
        """, (location.parent_kod,))
        
        parent_row = cursor.fetchone()
        
        if not parent_row:
            raise HTTPException(status_code=404, detail=f"Nadřazená lokace s kódem '{location.parent_kod}' nebyla nalezena.")
        
        parent_pk_id, parent_path = parent_row
        
        # 2. Vložení nové lokace pomocí nalezeného parent_pk_id
        cursor.execute(
            "INSERT INTO geo_locations (parent_id, typ, nazev) VALUES (%s, %s, %s) RETURNING pk_id;",
            (parent_pk_id, location.typ, location.nazev)
        )
        new_id = cursor.fetchone()[0]
        
        # 3. Aktualizace ltree_path
        new_path = f"{parent_path}.{new_id}"
        cursor.execute("UPDATE geo_locations SET ltree_path = %s WHERE pk_id = %s;", (new_path, new_id))
        
        # 4. Vložení identifikátorů do tabulky ids
        for ident in location.identifikatory:
            cursor.execute(
                "INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s);",
                (new_id, ident.value, ident.type, ident.priority)
            )
        
        conn.commit()
        return {"message": f"Lokace '{location.nazev}' byla úspěšně vytvořena.", "ltree_path": new_path}
    except HTTPException: raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Chyba databáze: {str(e)}")
    finally:
        cursor.close()
        conn.close()


@app.delete("/location/{identifier_value}")
def delete_location(identifier_value: str):
    """Smaže lokaci podle jakéhokoliv známého identifikátoru (IČO, LAU1, atd.)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Nalezení interního ID a názvu lokace podle zadaného kódu
        cursor.execute("""
            SELECT gl.pk_id, gl.nazev, gl.typ
            FROM ids i
            JOIN geo_locations gl ON i.location_pk = gl.pk_id
            WHERE i.value = %s
            LIMIT 1;
        """, (identifier_value,))
        
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Nenalezeno.")
            
        pk_id, nazev_mazane_lokace, typ_lokace = row

        # Smazání
        cursor.execute("DELETE FROM geo_locations WHERE pk_id = %s;", (pk_id,))
        conn.commit()
        return {"message": f"{typ_lokace} '{nazev_mazane_lokace}' byl smazán."}
    except HTTPException: raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Chyba databáze: {str(e)}")
    finally:
        cursor.close()
        conn.close()


@app.get("/validate", response_model=VysledekValidace)
def validate_id(
    type: TypIdentifikatoru = Query(...),
    value: str = Query(...),
) -> VysledekValidace:
    """Ověří formát vstupu a kontrolní číslici IČO.

    'format_valid' kontroluje přesný formát, 'accepted' povoluje i drobné úpravy.
    'valid' navíc zohledňuje kontrolní číslici.
    """
    return overit_identifikator(type, value)


def sestavit_vysledky_hledani(
    cursor: psycopg2.extensions.cursor, shody: List[Tuple[int, str, str, str]],
    location_type: Optional[str] = None,
) -> List[Dict[str, object]]:
    """Vrátí přímé shody i předky se stejným kódem, typem a mírou shody."""
    nalezene_pk = {pk for pk, _, _, _ in shody}

    cursor.execute("""
        SELECT DISTINCT u.pk_id, u.nazev, u.typ, u.ltree_path,
                        nlevel(u.ltree_path) AS hloubka
        FROM geo_locations n
        JOIN geo_locations u ON u.ltree_path @> n.ltree_path OR u.pk_id = n.pk_id
        WHERE n.pk_id = ANY(%s)
        ORDER BY hloubka DESC, u.pk_id;
    """, (list(nalezene_pk),))
    uzly = {}
    predci = {}
    for pk, nazev, typ, cesta, _ in cursor.fetchall():
        uzly[pk] = {"nazev": nazev, "typ_uzlu": typ}
        predci[pk] = [int(rodic_pk) for rodic_pk in str(cesta).split(".")[:-1]] if cesta else []

    for pk, uzel in uzly.items():
        rodice = []
        for rodic_pk in predci[pk]:
            rodic = uzly[rodic_pk]
            rodice.append(f"{rodic['nazev']} ({rodic['typ_uzlu']})")
        uzel["cesta"] = " > ".join(rodice) if rodice else "Kořenový uzel"

    # Přímé shody zůstávají první.
    poradi = list(shody)
    for rodic_pk in uzly:
        for pk, kod, typ, shoda in shody:
            if rodic_pk in predci[pk]:
                poradi.append((rodic_pk, kod, typ, shoda))

    vysledky = []
    pouzite_shody = set()
    for pk, kod, typ, shoda in poradi:
        if location_type and uzly[pk]["typ_uzlu"] != location_type:
            continue
        klic = (pk, kod, typ)
        if klic in pouzite_shody:
            continue
        pouzite_shody.add(klic)
        vysledky.append({
            "nazev": uzly[pk]["nazev"], "kod": kod, "typ": typ,
            "typ_uzlu": uzly[pk]["typ_uzlu"], "shoda": shoda, "cesta": uzly[pk]["cesta"],
        })
    return vysledky


# --- VYHLEDÁVACÍ ENDPOINT ---

@app.get("/search/{query}")
def search_id(
    query: str,
    search_type: Optional[str] = Query(None, regex="^(ico|zuj|lau2|nuts3|lau1|ruian|qcode|geonames|momc|kod_cobce|psc|mesta_obce|kod_ku)$"),
    location_type: Optional[str] = Query(None, regex="^(kraj|okres|obec|mestska_cast|cast_obce|katastralni_uzemi)$"),
) -> Dict[str, object]:
    conn = get_db_connection()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn:
            with conn.cursor() as cursor:
                try:
                    schema = nacist_aktivni_schema(cursor)
                except (ValueError, psycopg2.errors.UndefinedTable) as chyba:
                    raise HTTPException(status_code=503, detail="Aktivní datové schéma není nastavené.") from chyba
                cursor.execute(
                    sql.SQL("SET LOCAL search_path TO {schema}, public, pg_temp").format(
                        schema=sql.Identifier(schema)
                    )
                )

                db_type_filter = None
                if search_type:
                    search_type = search_type.lower()
                    if search_type in ['zuj', 'lau2']: db_type_filter = 'LAU2'
                    elif search_type == 'ico': db_type_filter = 'ICO'
                    elif search_type == 'nuts3': db_type_filter = 'NUTS3'
                    elif search_type == 'lau1': db_type_filter = 'LAU1'
                    elif search_type == 'ruian': db_type_filter = 'RUIAN'
                    elif search_type == 'qcode': db_type_filter = 'QCODE'
                    elif search_type == 'geonames': db_type_filter = 'GEONAMES'
                    elif search_type == 'momc': db_type_filter = 'MOMC'
                    elif search_type == 'kod_cobce': db_type_filter = 'KOD_COBCE'
                    elif search_type == 'psc': db_type_filter = 'PSC'
                    elif search_type == 'mesta_obce': db_type_filter = 'MESTA_OBCE'
                    elif search_type == 'kod_ku': db_type_filter = 'KOD_KU'

                hledany_dotaz = query.replace(" ", "").strip()
                db_location_filter = location_type.upper() if location_type else None
                # Kandidát musí mít požadovaný typ sám nebo mezi předky.
                # Filtr před LIMIT zabrání tomu, aby fuzzy místa zabrala jiná území.
                location_condition = """
                    AND EXISTS (
                        SELECT 1 FROM geo_locations u
                        WHERE u.typ = %s
                          AND (u.ltree_path @> gl.ltree_path OR u.pk_id = gl.pk_id)
                    )
                """ if db_location_filter else ""

                # exact match
                hledane_hodnoty = [hledany_dotaz]
                if hledany_dotaz.isdigit() and len(hledany_dotaz) < 8:
                    hledane_hodnoty.append(hledany_dotaz.zfill(8))

                sql_exact = """
                    SELECT gl.pk_id, i.value, i.type, i.priority
                    FROM ids i
                    JOIN geo_locations gl ON i.location_pk = gl.pk_id
                    WHERE i.value = ANY(%s)
                """
                params_exact = [hledane_hodnoty]

                if db_type_filter:
                    sql_exact += " AND i.type = %s"
                    params_exact.append(db_type_filter)
                if db_location_filter:
                    sql_exact += location_condition
                    params_exact.append(db_location_filter)

                sql_exact += " ORDER BY i.priority DESC;"

                cursor.execute(sql_exact, tuple(params_exact))
                presne_vysledky = cursor.fetchall()

                if presne_vysledky:
                    shody = [(pk, kod, typ, "100 %") for pk, kod, typ, _ in presne_vysledky]
                    response_data = sestavit_vysledky_hledani(cursor, shody, db_location_filter)

                    return {
                        "status": "exact_match",
                        "filter": db_type_filter if db_type_filter else "all",
                        "location_filter": db_location_filter if db_location_filter else "all",
                        "count": len(response_data),
                        "results": response_data
                    }

                # FUZZY VYHLEDÁVÁNÍ
                sql_fuzzy_final = """
                    SELECT gl.pk_id, i.value, i.type, (i.value <-> %s) as vzdalenost
                    FROM ids i
                    JOIN geo_locations gl ON i.location_pk = gl.pk_id
                    WHERE TRUE
                """
                query_params = [hledany_dotaz]

                if db_type_filter:
                    sql_fuzzy_final += " AND i.type = %s "
                    query_params.append(db_type_filter)
                if db_location_filter:
                    sql_fuzzy_final += location_condition
                    query_params.append(db_location_filter)

                sql_fuzzy_final += " ORDER BY (i.value <-> %s) ASC, i.priority DESC LIMIT 5;"
                query_params.append(hledany_dotaz)

                cursor.execute(sql_fuzzy_final, tuple(query_params))
                vysledky_fuzzy = cursor.fetchall()

                if not vysledky_fuzzy:
                    raise HTTPException(status_code=404, detail="Nic nenalezeno.")

                shody = []
                for pk, kod, typ, vzdalenost in vysledky_fuzzy:
                    shoda_procenta = round((1 - vzdalenost) * 100, 2)
                    if shoda_procenta < 10:
                        continue
                    shody.append((pk, kod, typ, f"{shoda_procenta} %"))

                if not shody:
                    raise HTTPException(status_code=404, detail="Nic dostatečně podobného nenalezeno.")
                response_data = sestavit_vysledky_hledani(cursor, shody, db_location_filter)

                return {
                    "status": "fuzzy_match",
                    "filter": db_type_filter if db_type_filter else "all",
                    "location_filter": db_location_filter if db_location_filter else "all",
                    "count": len(response_data),
                    "results": response_data
                }
    finally:
        conn.close()
