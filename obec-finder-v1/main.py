import time
import csv
import json
import os
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict

app = FastAPI()

def vycistit_nazev(nazev_s_typem):
    # orezani predpon
    predpony = [
        "Hlavní město ",
        "Statutární město ", 
        "Město ",
        "Městys ",
        "Obec "
    ]
    
    cisty_nazev = nazev_s_typem.strip()
    
    for p in predpony:
        if cisty_nazev.startswith(p):
            return cisty_nazev[len(p):]
            
    return cisty_nazev

def nahrat_ico(cursor):
    print("Kontroluji IČO data...")
    
    cursor.execute("SELECT count(*) FROM ids WHERE type='ICO';")
    if cursor.fetchone()[0] > 0:
        print("IČO data už v databázi jsou")
        return

    cursor.execute("SELECT nazev, pk_id FROM geo_locations WHERE typ = 'OBEC';")
    obce_mapa = {row[0]: row[1] for row in cursor.fetchall()}
    
    log_soubor = "chyby_parovani.txt"
    
    try:
        with open('uzemni-samosprava_obce_30-11-2025.csv', 'r', encoding='utf-8') as f_in, \
             open(log_soubor, 'w', encoding='utf-8') as f_log:
            
            reader = csv.reader(f_in, delimiter=';') 
            next(reader)
            f_log.write("IČO;Původní název;Očištěný název;Důvod\n")
            
            vlozeno = 0
            chyby = 0
            
            for radek in reader:
                try:
                    ico_hodnota = radek[0].zfill(8)
                    nazev_original = radek[1]
                    
                    nazev_hledany = vycistit_nazev(nazev_original)
                    pk_obce = obce_mapa.get(nazev_hledany)
                    
                    if pk_obce:
                        cursor.execute(
                            "INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s)",
                            (pk_obce, ico_hodnota, 'ICO', 80)
                        )
                        vlozeno += 1
                    else:
                        f_log.write(f"{ico_hodnota};{nazev_original};{nazev_hledany};Nenalezeno v DB\n")
                        chyby += 1
                        
                except IndexError:
                    continue
            
            print(f"IČO nahráno. Spárováno: {vlozeno}")
            print(f"Počet chyb: {chyby}. Detaily v souboru '{log_soubor}'")

    except FileNotFoundError:
        print("Chyba: Soubor CSV s IČO nenalezen.")


def nahrat_cis_kody(cursor):
    """Přečte CIS soubory a nalepí IDs na Kraje a Okresy"""
    print("Kontroluji data pro Kraje a Okresy (CIS)...")
    
    cursor.execute("SELECT count(*) FROM ids WHERE type IN ('NUTS3', 'LAU1', 'RUIAN');")
    if cursor.fetchone()[0] > 0:
        print("Data pro Kraje a Okresy už v databázi jsou")
        return

    cursor.execute("SELECT typ, nazev, pk_id FROM geo_locations WHERE typ IN ('KRAJ', 'OKRES');")
    db_uzly = {}
    for row in cursor.fetchall():
        typ, nazev, pk_id = row
        nazev_norm = nazev.replace(" - ", "-").strip()
        db_uzly[(typ, nazev_norm)] = pk_id

    # Ošetření pro Prahu z ČSÚ souborů
    if ('OKRES', 'Hlavní město Praha') in db_uzly:
        db_uzly[('OKRES', 'Praha')] = db_uzly[('OKRES', 'Hlavní město Praha')]

    try:
        # CIS0100
        with open('CIS0100_CS.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                nazev = row[5].replace(" - ", "-").strip()
                if nazev == "Extra-Regio": continue
                pk_id = db_uzly.get(('KRAJ', nazev))
                if pk_id:
                    cursor.execute("INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s);", (pk_id, row[8], 'NUTS3', 80))
    except FileNotFoundError:
        print("Chyba: CIS0100_CS.csv nenalezen.")

    try:
        # CIS0101 (Okresy - LAU1 a RUIAN)
        with open('CIS0101_CS.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                nazev = row[5].replace(" - ", "-").strip()
                if nazev == "Extra-Regio": continue
                pk_id = db_uzly.get(('OKRES', nazev))
                if pk_id:
                    # Sloupec 9 = okres_lau, Sloupec 11 = kod_ruian
                    cursor.execute("INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s);", (pk_id, row[9], 'LAU1', 80))
                    cursor.execute("INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s);", (pk_id, row[11], 'RUIAN', 80))
    except FileNotFoundError:
        print("Chyba: CIS0101_CS.csv nenalezen.")

    print("Data pro Kraje a Okresy byla nahrána.")


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )


@app.on_event("startup")
def startup_db():
    conn = None
    for _ in range(10):
        try:
            conn = get_db_connection()
            break
        except psycopg2.OperationalError:
            time.sleep(2)
    
    if not conn: return
    cursor = conn.cursor()

    # ROZŠÍŘENÍ PRO FUZZY SEARCH (Trigrams) A HIERARCHII (ltree)
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    cursor.execute("CREATE EXTENSION IF NOT EXISTS ltree;")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS geo_locations (
            pk_id SERIAL PRIMARY KEY,
            parent_id INTEGER REFERENCES geo_locations(pk_id) ON DELETE CASCADE,
            typ VARCHAR(50) NOT NULL,
            nazev VARCHAR(255) NOT NULL,
            ltree_path ltree
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ids (
            pk SERIAL PRIMARY KEY,
            location_pk INTEGER REFERENCES geo_locations(pk_id) ON DELETE CASCADE,
            value VARCHAR(50) NOT NULL,
            type VARCHAR(20) NOT NULL,
            priority INTEGER DEFAULT 0
        );
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ids_value_btree ON ids (value);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ids_value_gin ON ids USING GIN (value gin_trgm_ops);")
    cursor.execute("CREATE INDEX IF NOT EXISTS path_gist_idx ON geo_locations USING GIST (ltree_path);")
    
    conn.commit()

    # NAHRÁNÍ DAT
    cursor.execute("SELECT count(*) FROM geo_locations;")
    if cursor.fetchone()[0] == 0:
        
        print("Vytvářím stromovou strukturu z master_geo.json...")
        try:
            with open('master_geo.json', 'r', encoding='utf-8') as f:
                master_data = json.load(f)
            
            id_map = {}
            for uroven in ["KRAJ", "OKRES", "OBEC"]:
                for uzel in master_data:
                    if uzel['typ'] == uroven:
                        db_parent_id = id_map.get(uzel['parent_id']) if uzel['parent_id'] else None
                        
                        cursor.execute(
                            "INSERT INTO geo_locations (parent_id, typ, nazev) VALUES (%s, %s, %s) RETURNING pk_id;",
                            (db_parent_id, uzel['typ'], uzel['nazev'])
                        )
                        nove_db_id = cursor.fetchone()[0]
                        id_map[uzel['id']] = nove_db_id
                        
                        if db_parent_id:
                            cursor.execute("SELECT ltree_path FROM geo_locations WHERE pk_id = %s;", (db_parent_id,))
                            nova_cesta = f"{cursor.fetchone()[0]}.{nove_db_id}"
                        else:
                            nova_cesta = str(nove_db_id)
                            
                        cursor.execute("UPDATE geo_locations SET ltree_path = %s WHERE pk_id = %s;", (nova_cesta, nove_db_id))
        except FileNotFoundError:
            print("Chyba: master_geo.json nenalezen.")

        print("Nahrávám ZUJ data ze zuj-name.csv...")
        try:
            cursor.execute("SELECT nazev, pk_id FROM geo_locations WHERE typ = 'OBEC';")
            obce_mapa_zuj = {row[0]: row[1] for row in cursor.fetchall()}

            with open('zuj-name.csv', 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                
                for radek in reader:
                    lau2_code = radek[3] 
                    nazev = radek[5]     
                    
                    pk_obce = obce_mapa_zuj.get(nazev)
                    if pk_obce:
                        cursor.execute(
                            "INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s)",
                            (pk_obce, lau2_code, 'LAU2', 100)
                        )
            print("ZUJ data spárována.")
        except FileNotFoundError:
            print("Chyba: zuj-name.csv nenalezen.")
        except Exception as e:
            print(f"Chyba při nahrávání ZUJ: {e}")

    # Volání parsovacích funkcí
    nahrat_ico(cursor)
    nahrat_cis_kody(cursor)
    
    conn.commit()
    cursor.close()
    conn.close()


@app.get("/search/{query}")
def search_id(
    query: str, 
    search_type: str = Query(None, regex="^(ico|zuj|lau2|nuts3|lau1|ruian)$") 
):
    conn = get_db_connection()
    cursor = conn.cursor()

    db_type_filter = None
    if search_type:
        search_type = search_type.lower()
        if search_type in ['zuj', 'lau2']:
            db_type_filter = 'LAU2'
        elif search_type == 'ico':
            db_type_filter = 'ICO'
        elif search_type == 'nuts3':
            db_type_filter = 'NUTS3'
        elif search_type == 'lau1':
            db_type_filter = 'LAU1'
        elif search_type == 'ruian':
            db_type_filter = 'RUIAN'

    # exact match
    hledane_hodnoty = [query]
    if query.isdigit() and len(query) < 8:
        hledane_hodnoty.append(query.zfill(8))

    sql_exact = """
        SELECT gl.nazev, i.value, i.type, i.priority, gl.typ, gl.ltree_path 
        FROM ids i
        JOIN geo_locations gl ON i.location_pk = gl.pk_id
        WHERE i.value = ANY(%s)
    """
    params_exact = [hledane_hodnoty]

    if db_type_filter:
        sql_exact += " AND i.type = %s"
        params_exact.append(db_type_filter)
    
    sql_exact += " ORDER BY i.priority DESC;"

    cursor.execute(sql_exact, tuple(params_exact))
    presne_vysledky = cursor.fetchall()

    if presne_vysledky:
        response_data = []
        for row in presne_vysledky:
            nazev, kod, typ_kodu, priorita, typ_uzlu, ltree_cesta = row

            rodice_seznam = []
            if ltree_cesta:
                sql_rodice = "SELECT nazev, typ FROM geo_locations WHERE ltree_path @> %s AND ltree_path != %s ORDER BY nlevel(ltree_path) ASC;"
                cursor.execute(sql_rodice, (ltree_cesta, ltree_cesta))
                for r in cursor.fetchall():
                    rodice_seznam.append(f"{r[0]} ({r[1]})")

            response_data.append({
                "obec": nazev,
                "kod": kod,
                "typ": typ_kodu,
                "typ_uzlu": typ_uzlu,
                "shoda": "100 %",
                "cesta": " > ".join(rodice_seznam) if rodice_seznam else "Kořenový uzel"
            })

        cursor.close()
        conn.close()
        return {
            "status": "exact_match",
            "filter": db_type_filter if db_type_filter else "all",
            "count": len(response_data),
            "results": response_data
        }

    # FUZZY VYHLEDÁVÁNÍ
    sql_fuzzy_final = """
        SELECT gl.nazev, i.value, i.type, (i.value <-> %s) as vzdalenost, gl.typ, gl.ltree_path
        FROM ids i
        JOIN geo_locations gl ON i.location_pk = gl.pk_id
    """
    query_params = [query]
    
    if db_type_filter:
        sql_fuzzy_final += " WHERE i.type = %s "
        query_params.append(db_type_filter)
        
    sql_fuzzy_final += " ORDER BY (i.value <-> %s) ASC, i.priority DESC LIMIT 5;"
    query_params.append(query)

    cursor.execute(sql_fuzzy_final, tuple(query_params))
    vysledky_fuzzy = cursor.fetchall()

    if not vysledky_fuzzy:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Nic nenalezeno.")

    response_data = []
    for row in vysledky_fuzzy:
        nazev, kod, typ_kodu, vzdalenost, typ_uzlu, ltree_cesta = row
        shoda_procenta = round((1 - vzdalenost) * 100, 2)
        if shoda_procenta < 10: continue

        rodice_seznam = []
        if ltree_cesta:
            sql_rodice = "SELECT nazev, typ FROM geo_locations WHERE ltree_path @> %s AND ltree_path != %s ORDER BY nlevel(ltree_path) ASC;"
            cursor.execute(sql_rodice, (ltree_cesta, ltree_cesta))
            for r in cursor.fetchall():
                rodice_seznam.append(f"{r[0]} ({r[1]})")

        response_data.append({
            "obec": nazev,
            "kod": kod,
            "typ": typ_kodu,
            "typ_uzlu": typ_uzlu,
            "shoda": f"{shoda_procenta} %",
            "cesta": " > ".join(rodice_seznam) if rodice_seznam else "Kořenový uzel"
        })

    cursor.close()
    conn.close()

    if not response_data:
        raise HTTPException(status_code=404, detail="Nic dostatečně podobného nenalezeno.")

    return {
        "status": "fuzzy_match",
        "filter": db_type_filter if db_type_filter else "all",
        "results": response_data
    }