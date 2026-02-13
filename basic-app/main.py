import time
import csv
import os
import psycopg2
from fastapi import FastAPI, HTTPException
from typing import List, Dict

app = FastAPI()

def vycistit_nazev(nazev_s_typem):
    # musime orezat predpony
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

    cursor.execute("SELECT nazev_obce, pk FROM municipality;")
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
                    ico_hodnota = radek[0]
                    nazev_original = radek[1]
                    
                    nazev_hledany = vycistit_nazev(nazev_original)
                    pk_obce = obce_mapa.get(nazev_hledany)
                    
                    if pk_obce:
                        cursor.execute(
                            "INSERT INTO ids (obec_pk, value, type, priority) VALUES (%s, %s, %s, %s)",
                            (pk_obce, ico_hodnota, 'ICO', 80)
                        )
                        vlozeno += 1
                    else:
                        # NENALEZENO
                        f_log.write(f"{ico_hodnota};{nazev_original};{nazev_hledany};Nenalezeno v DB\n")
                        chyby += 1
                        
                except IndexError:
                    # poskozeny radek
                    continue
            
            print(f"IČO nahráno. Spárováno: {vlozeno}")
            print(f"Počet chyb: {chyby}. Detaily v souboru '{log_soubor}'")

    except FileNotFoundError:
        print("Chyba: Soubor CSV nenalezen.")

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

    # ROZŠÍŘENÍ PRO FUZZY SEARCH (Trigrams)
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    
    # Tabulka Municipality
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS municipality (
            pk SERIAL PRIMARY KEY,
            nazev_obce VARCHAR(255) NOT NULL
        );
    """)

    # Tabulka IDs (value, type, priority)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ids (
            pk SERIAL PRIMARY KEY,
            obec_pk INTEGER REFERENCES municipality(pk) ON DELETE CASCADE,
            value VARCHAR(50) NOT NULL,
            type VARCHAR(20) NOT NULL,
            priority INTEGER DEFAULT 0
        );
    """)

    
    # B-Tree index pro presnou shodu
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ids_value_btree ON ids (value);")
    
    # GIN index pro fuzzy search
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ids_value_gin ON ids USING GIN (value gin_trgm_ops);")
    
    conn.commit()

    # NAHRÁNÍ DAT (Pokud je tabulka prázdná)
    cursor.execute("SELECT count(*) FROM municipality;")
    if cursor.fetchone()[0] == 0:
        print("Nahrávám data z zuj-name.csv...")
        try:
            with open('zuj-name.csv', 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                
                for radek in reader:
                    lau2_code = radek[3] # ZUJ
                    nazev = radek[5]     # Název
                    
                    # vlozeni obce
                    cursor.execute(
                        "INSERT INTO municipality (nazev_obce) VALUES (%s) RETURNING pk;",
                        (nazev,)
                    )
                    novy_pk_obce = cursor.fetchone()[0]

                    # vlozeni id
                    cursor.execute(
                        """
                        INSERT INTO ids (obec_pk, value, type, priority) 
                        VALUES (%s, %s, %s, %s)
                        """,
                        (novy_pk_obce, lau2_code, 'LAU2', 100)
                    )
            
            conn.commit()
            print("Data nahrána a indexována.")
            
        except FileNotFoundError:
            print("Chyba: soubor CSV nenalezen.")
        except Exception as e:
            print(f"Chyba při nahrávání: {e}")
            conn.rollback()
    
    
    nahrat_ico(cursor)
    
    conn.commit()
    cursor.close()
    conn.close()


@app.get("/search/{query}")
def search_id(
    query: str, 
    # parametr 'type'. Defaultně je None (hledá vše).
    search_type: str = Query(None, regex="^(ico|zuj|lau2)$") 
):
    conn = get_db_connection()
    cursor = conn.cursor()

    db_type_filter = None
    if search_type:
        search_type = search_type.lower()
        if search_type == 'zuj' or search_type == 'lau2':
            db_type_filter = 'LAU2'
        elif search_type == 'ico':
            db_type_filter = 'ICO'

    # exact match
    hledane_hodnoty = [query]
    if query.isdigit() and len(query) < 8:
        hledane_hodnoty.append(query.zfill(8))

    sql_exact = """
        SELECT m.nazev_obce, i.value, i.type, i.priority 
        FROM ids i
        JOIN municipality m ON i.obec_pk = m.pk
        WHERE i.value = ANY(%s)
    """
    params_exact = [hledane_hodnoty]

    # Dynamické přidání filtru
    if db_type_filter:
        sql_exact += " AND i.type = %s"
        params_exact.append(db_type_filter)
    
    # Seřazení
    sql_exact += " ORDER BY i.priority DESC;"

    cursor.execute(sql_exact, tuple(params_exact))
    presne_vysledky = cursor.fetchall()

    if presne_vysledky:
        cursor.close()
        conn.close()
        
        response_data = []
        for row in presne_vysledky:
            response_data.append({
                "obec": row[0],
                "kod": row[1],
                "typ": row[2],
                "shoda": "100 %"
            })

        return {
            "status": "exact_match",
            "filter": db_type_filter if db_type_filter else "all",
            "count": len(response_data),
            "results": response_data
        }

    # FUZZY VYHLEDÁVÁNÍ
    
    sql_fuzzy = """
        SELECT m.nazev_obce, i.value, i.type, (i.value <-> %s) as vzdalenost
        FROM ids i
        JOIN municipality m ON i.obec_pk = m.pk
    """
    params_fuzzy = [query]

    if db_type_filter:
        sql_fuzzy += " WHERE i.type = %s"
        params_fuzzy.append(db_type_filter)
    

    sql_fuzzy += " ORDER BY (i.value <-> %s) ASC, i.priority DESC LIMIT 5;"
    params_fuzzy.append(query) # Třetí parametr (znovu query pro ORDER BY)
    
    sql_fuzzy_final = """
        SELECT m.nazev_obce, i.value, i.type, (i.value <-> %s) as vzdalenost
        FROM ids i
        JOIN municipality m ON i.obec_pk = m.pk
    """
    query_params = [query]
    
    if db_type_filter:
        sql_fuzzy_final += " WHERE i.type = %s "
        query_params.append(db_type_filter)
        
    sql_fuzzy_final += " ORDER BY (i.value <-> %s) ASC, i.priority DESC LIMIT 5;"
    query_params.append(query)

    cursor.execute(sql_fuzzy_final, tuple(query_params))
    vysledky_fuzzy = cursor.fetchall()
    
    cursor.close()
    conn.close()

    if not vysledky_fuzzy:
        raise HTTPException(status_code=404, detail="Nic nenalezeno.")

    response_data = []
    for row in vysledky_fuzzy:
        shoda_procenta = round((1 - row[3]) * 100, 2)
        if shoda_procenta < 10: continue

        response_data.append({
            "obec": row[0],
            "kod": row[1],
            "typ": row[2],
            "shoda": f"{shoda_procenta} %"
        })

    if not response_data:
        raise HTTPException(status_code=404, detail="Nic dostatečně podobného nenalezeno.")

    return {
        "status": "fuzzy_match",
        "filter": db_type_filter if db_type_filter else "all",
        "results": response_data
    }