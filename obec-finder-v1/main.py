import time
import csv
import os
import re
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List

app = FastAPI()

# --- 1. STATICKÁ DATA PRO VYTVOŘENÍ STROMU ---
MAPA_KRAJU = {
    "Hlavní město Praha": ["Praha"],
    "Středočeský kraj": ["Benešov", "Beroun", "Kladno", "Kolín", "Kutná Hora", "Mělník", "Mladá Boleslav", "Nymburk", "Praha-východ", "Praha-západ", "Příbram", "Rakovník"],
    "Jihočeský kraj": ["České Budějovice", "Český Krumlov", "Jindřichův Hradec", "Písek", "Prachatice", "Strakonice", "Tábor"],
    "Plzeňský kraj": ["Domažlice", "Klatovy", "Plzeň-město", "Plzeň-jih", "Plzeň-sever", "Rokycany", "Tachov"],
    "Karlovarský kraj": ["Cheb", "Karlovy Vary", "Sokolov"],
    "Ústecký kraj": ["Děčín", "Chomutov", "Litoměřice", "Louny", "Most", "Teplice", "Ústí nad Labem"],
    "Liberecký kraj": ["Česká Lípa", "Jablonec nad Nisou", "Liberec", "Semily"],
    "Královéhradecký kraj": ["Hradec Králové", "Jičín", "Náchod", "Rychnov nad Kněžnou", "Trutnov"],
    "Pardubický kraj": ["Chrudim", "Pardubice", "Svitavy", "Ústí nad Orlicí"],
    "Kraj Vysočina": ["Havlíčkův Brod", "Jihlava", "Pelhřimov", "Třebíč", "Žďár nad Sázavou"],
    "Jihomoravský kraj": ["Blansko", "Brno-město", "Brno-venkov", "Břeclav", "Hodonín", "Vyškov", "Znojmo"],
    "Olomoucký kraj": ["Jeseník", "Olomouc", "Prostějov", "Přerov", "Šumperk"],
    "Zlínský kraj": ["Kroměříž", "Uherské Hradiště", "Vsetín", "Zlín"],
    "Moravskoslezský kraj": ["Bruntál", "Frýdek-Místek", "Karviná", "Nový Jičín", "Opava", "Ostrava-město"]
}


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

# -------------------------------------------------

# --- POMOCNÉ FUNKCE ---
def vycistit_nazev(nazev_s_typem):
    predpony = ["Hlavní město ", "Statutární město ", "Město ", "Městys ", "Obec "]
    cisty_nazev = nazev_s_typem.strip()
    
    for p in predpony:
        if cisty_nazev.startswith(p):
            return cisty_nazev[len(p):]
            
    return cisty_nazev

def normalizovat_okres(okres_str):
    return re.sub(r'\s*-\s*', '-', okres_str.strip())


# --- PARSOVACÍ FUNKCE ---
def nahrat_ico(cursor):
    """Nahrání IČO pomocí složeného klíče (Název obce + Název okresu)"""
    print("Kontroluji IČO data...")
    
    cursor.execute("SELECT count(*) FROM ids WHERE type='ICO';")
    if cursor.fetchone()[0] > 0:
        print("IČO data už v databázi jsou")
        return

    cursor.execute("""
        SELECT o.nazev, ok.nazev, o.pk_id 
        FROM geo_locations o 
        JOIN geo_locations ok ON o.parent_id = ok.pk_id 
        WHERE o.typ = 'OBEC' AND ok.typ = 'OKRES';
    """)
    obce_mapa = {(row[0].lower(), row[1].lower()): row[2] for row in cursor.fetchall()}
        
    vlozeno = 0
    try:
        with open('uzemni-samosprava_obce_30-11-2025.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader)
            
            for row in reader:
                ico = row[0].zfill(8)
                nazev_obce = vycistit_nazev(row[1]).lower()
                nazev_okresu = normalizovat_okres(row[2]).lower()
                
                # Praha
                if "hlavní město praha" in nazev_okresu:
                    nazev_okresu = "praha"

                # Hledáni podle jmena a okresu
                pk_id = obce_mapa.get((nazev_obce, nazev_okresu))
                
                if pk_id:
                    cursor.execute("INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, 'ICO', 80);", (pk_id, ico))
                    vlozeno += 1
                    
        print(f"IČO úspěšně nahráno a jmenovci vyřešeni. Spárováno: {vlozeno}")
    except FileNotFoundError:
        pass

def nahrat_cis_kody(cursor):
    """Přečte CIS soubory a nalepí IDs na Kraje a Okresy"""
    print("Kontroluji data pro Kraje a Okresy (CIS)...")
    
    cursor.execute("SELECT count(*) FROM ids WHERE type IN ('NUTS3', 'LAU1', 'RUIAN');")
    if cursor.fetchone()[0] > 0:
        print("Data pro Kraje a Okresy už v databázi jsou")
        return

    cursor.execute("SELECT typ, nazev, pk_id FROM geo_locations WHERE typ IN ('KRAJ', 'OKRES');")
    db_uzly = { (typ, nazev.replace(" - ", "-").strip()): pk_id for typ, nazev, pk_id in cursor.fetchall() }

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
        pass

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
        pass

def nahrat_wikidata_qcodes(cursor):
    """Přečte stažené CSV, napáruje Q-kódy a GeoNames IDs (s ochranou proti duplicitám)."""
    print("Kontroluji Wikidata Q-kódy a GeoNames IDs...")
    
    cursor.execute("SELECT count(*) FROM ids WHERE type='QCODE';")
    if cursor.fetchone()[0] > 0:
        return

    cursor.execute("SELECT count(*) FROM geo_locations WHERE typ='OBEC';")
    celkem_obci_v_db = cursor.fetchone()[0]

    cursor.execute("SELECT value, location_pk FROM ids WHERE type='LAU2';")
    lau2_mapa = {row[0]: row[1] for row in cursor.fetchall()}

    sparovane_pk = set()
    nesparovane_wikidata = []
    
    # Paměť, abychom nevkládali duplicity
    vlozene_qcodes = set()
    vlozene_geonames = set()
    
    pocet_qcodes = 0
    pocet_geonames = 0

    try:
        with open('wikidata_obce.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                lau2 = row['lau2']
                qcode = row['qcode']
                geonames = row.get('geonames', '')
                
                if lau2 in lau2_mapa:
                    pk_lokace = lau2_mapa[lau2]
                    
                    # 1. Vložení Q-kódu (POUZE POKUD TAM JEŠTĚ NENÍ)
                    qcode_klic = (pk_lokace, qcode)
                    if qcode_klic not in vlozene_qcodes:
                        cursor.execute(
                            "INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s);",
                            (pk_lokace, qcode, 'QCODE', 90)
                        )
                        vlozene_qcodes.add(qcode_klic)
                        pocet_qcodes += 1
                        sparovane_pk.add(pk_lokace)
                    
                    # 2. Vložení GeoNames ID (POUZE POKUD TAM JEŠTĚ NENÍ)
                    if geonames:
                        geonames_klic = (pk_lokace, geonames)
                        if geonames_klic not in vlozene_geonames:
                            cursor.execute(
                                "INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, %s, %s);",
                                (pk_lokace, geonames, 'GEONAMES', 70)
                            )
                            vlozene_geonames.add(geonames_klic)
                            pocet_geonames += 1
                else:
                    nesparovane_wikidata.append((qcode, lau2))
                    
        # Počítáme úspěšnost podle unikátních obcí, které dostaly alespoň nějaký kód
        uspesnost = (len(sparovane_pk) / celkem_obci_v_db) * 100 if celkem_obci_v_db > 0 else 0
        
        print("\n--- REPORT: Wikidata & GeoNames ---")
        print(f"Celkem obcí v databázi: {celkem_obci_v_db}")
        print(f"Unikátních obcí s Q-kódem: {len(sparovane_pk)}")
        print(f"Celkem uloženo Q-kódů: {pocet_qcodes}")
        print(f"Celkem uloženo GeoNames ID: {pocet_geonames}")
        print(f"Úspěšnost pokrytí obcí z Wikidat: {uspesnost:.2f} %")
        print("-------------------------------\n")

    except FileNotFoundError:
        print("Chyba: Soubor wikidata_obce.csv nenalezen. Přeskakuji.")


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )


# -------------------------------------------
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

    # --- NOVÁ TVORBA STROMU POMOCÍ VAZ0043 A MAPA_KRAJU ---
    cursor.execute("SELECT count(*) FROM geo_locations;")
    if cursor.fetchone()[0] == 0:
        print("Vytvářím stromovou strukturu přímo z VAZ dat a mapy krajů...")
        
        # 1. Vložení Krajů
        kraje_db = {}
        for kraj_nazev in MAPA_KRAJU.keys():
            cursor.execute("INSERT INTO geo_locations (parent_id, typ, nazev) VALUES (NULL, 'KRAJ', %s) RETURNING pk_id;", (kraj_nazev,))
            pk_id = cursor.fetchone()[0]
            cursor.execute("UPDATE geo_locations SET ltree_path = %s WHERE pk_id = %s;", (str(pk_id), pk_id))
            kraje_db[kraj_nazev] = pk_id

        # 2. Vložení Okresů
        okresy_db = {}
        for kraj_nazev, okresy in MAPA_KRAJU.items():
            parent_kraj_id = kraje_db[kraj_nazev]
            for okres_nazev in okresy:
                cursor.execute("INSERT INTO geo_locations (parent_id, typ, nazev) VALUES (%s, 'OKRES', %s) RETURNING pk_id;", (parent_kraj_id, okres_nazev))
                pk_id = cursor.fetchone()[0]
                ltree = f"{parent_kraj_id}.{pk_id}"
                cursor.execute("UPDATE geo_locations SET ltree_path = %s WHERE pk_id = %s;", (ltree, pk_id))
                okresy_db[okres_nazev.lower()] = pk_id

        # 3. Vložení Obcí z VAZ s přiřazením ZUJ (LAU2) do správného okresu
        try:
            with open('VAZ0043_0101_CS.csv', 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                
                vlozeno = 0
                for row in reader:
                    # Sloupec 4 je ZUJ, 5 je Obec, 9 je Okres
                    zuj = row[4]
                    nazev_obce = vycistit_nazev(row[5])
                    nazev_okresu = normalizovat_okres(row[9]).lower()
                    
                    if "hlavní město praha" in nazev_okresu:
                        nazev_okresu = "praha"
                        
                    parent_okres_id = okresy_db.get(nazev_okresu)
                    
                    if parent_okres_id:
                        # Vloženi obce pod svuj okres
                        cursor.execute("INSERT INTO geo_locations (parent_id, typ, nazev) VALUES (%s, 'OBEC', %s) RETURNING pk_id;", (parent_okres_id, nazev_obce))
                        obec_pk = cursor.fetchone()[0]
                        
                        # Generování cesty ltree
                        cursor.execute("SELECT ltree_path FROM geo_locations WHERE pk_id = %s;", (parent_okres_id,))
                        parent_path = cursor.fetchone()[0]
                        cursor.execute("UPDATE geo_locations SET ltree_path = %s WHERE pk_id = %s;", (f"{parent_path}.{obec_pk}", obec_pk))
                        
                        # pridani LAU2 kódu do ids
                        cursor.execute("INSERT INTO ids (location_pk, value, type, priority) VALUES (%s, %s, 'LAU2', 100);", (obec_pk, zuj))
                        vlozeno += 1
                        
            print(f"Strom a LAU2 kódy byly úspěšně nahrány pro {vlozeno} obcí.")
        except FileNotFoundError:
            pass

    # 4. Dodatečné volání doplňkových kódů
    nahrat_ico(cursor)
    nahrat_cis_kody(cursor)
    nahrat_wikidata_qcodes(cursor)
    
    conn.commit()
    cursor.close()
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


# --- VYHLEDÁVACÍ ENDPOINT ---

@app.get("/search/{query}")
def search_id(
    query: str, 
    # AKTUALIZACE REGEXU pro geonames
    search_type: str = Query(None, regex="^(ico|zuj|lau2|nuts3|lau1|ruian|qcode|geonames)$") 
):
    conn = get_db_connection()
    cursor = conn.cursor()

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