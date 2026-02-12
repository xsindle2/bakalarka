import time
import csv
import os
import psycopg2
from fastapi import FastAPI, HTTPException
from typing import List, Dict

app = FastAPI()

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
    # 1. Čekání na DB
    for _ in range(10):
        try:
            conn = get_db_connection()
            print("Připojeno k DB.")
            break
        except psycopg2.OperationalError:
            print("DB startuje, čekám...")
            time.sleep(2)
    
    if not conn:
        print("Nepodařilo se připojit k DB.")
        return

    cursor = conn.cursor()

    # 2. AKTIVACE ROZŠÍŘENÍ PRO FUZZY SEARCH (Trigrams)
    # Tohle je klíčové pro GIN index
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    # 3. VYTVOŘENÍ TABULEK (Podle návrhu vedoucího)
    
    # Tabulka Municipality (Obce)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS municipality (
            pk SERIAL PRIMARY KEY,
            nazev_obce VARCHAR(255) NOT NULL
        );
    """)

    # Tabulka IDs (Identifikátory)
    # value = samotný kód (např. '554782')
    # type = typ kódu (např. 'LAU2', 'ICO')
    # priority = váha (např. 100 pro LAU2, 50 pro IČO - vyšší číslo = vyšší priorita)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ids (
            pk SERIAL PRIMARY KEY,
            obec_pk INTEGER REFERENCES municipality(pk) ON DELETE CASCADE,
            value VARCHAR(50) NOT NULL,
            type VARCHAR(20) NOT NULL,
            priority INTEGER DEFAULT 0
        );
    """)

    # 4. VYTVOŘENÍ INDEXŮ (Klíč pro rychlost)
    
    # B-Tree index pro PŘESNOU shodu (rychlé =)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ids_value_btree ON ids (value);")
    
    # GIN index pro PODOBNOST (rychlé LIKE a %)
    # gin_trgm_ops je speciální operátor z pg_trgm
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ids_value_gin ON ids USING GIN (value gin_trgm_ops);")
    
    conn.commit()

    # 5. NAHRÁNÍ DAT (Pokud je tabulka prázdná)
    cursor.execute("SELECT count(*) FROM municipality;")
    if cursor.fetchone()[0] == 0:
        print("Nahrávám data z zuj-name.csv...")
        try:
            with open('zuj-name.csv', 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) # Přeskočit hlavičku
                
                for radek in reader:
                    lau2_code = radek[3] # ZUJ
                    nazev = radek[5]     # Název
                    
                    # A) Vložíme obec a získáme její ID (pk)
                    cursor.execute(
                        "INSERT INTO municipality (nazev_obce) VALUES (%s) RETURNING pk;",
                        (nazev,)
                    )
                    novy_pk_obce = cursor.fetchone()[0]

                    # B) Vložíme kód do tabulky IDs
                    # Typ nastavíme na 'LAU2', Prioritu třeba na 100
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
    
    cursor.close()
    conn.close()


# --- ENDPOINT ---

@app.get("/search/{query}")
def search_id(query: str):
    conn = get_db_connection()
    cursor = conn.cursor()

    # KROK 1: Přesná shoda (B-Tree Index) - Zůstává stejné
    # Je to nejrychlejší, tak to zkusíme první
    sql_exact = """
        SELECT m.nazev_obce, i.value, i.type 
        FROM ids i
        JOIN municipality m ON i.obec_pk = m.pk
        WHERE i.value = %s;
    """
    cursor.execute(sql_exact, (query,))
    presna = cursor.fetchone()

    if presna:
        cursor.close()
        conn.close()
        return {
            "status": "exact_match",
            "result": {
                "obec": presna[0],
                "kod": presna[1],
                "typ": presna[2]
            }
        }

    # KROK 2: Podobnost (GIN Index + Vzdálenost) - ZMĚNA ZDE
    # Místo operátoru % (který filtruje) použijeme <-> (který řadí).
    # <-> vrací vzdálenost (0 = stejné, 1 = úplně jiné).
    # My chceme co nejmenší vzdálenost, proto ORDER BY ... ASC
    
    sql_fuzzy = """
        SELECT m.nazev_obce, i.value, i.type, (i.value <-> %s) as vzdalenost
        FROM ids i
        JOIN municipality m ON i.obec_pk = m.pk
        ORDER BY (i.value <-> %s) ASC, i.priority DESC
        LIMIT 5;
    """
    
    cursor.execute(sql_fuzzy, (query, query))
    vysledky = cursor.fetchall()
    
    cursor.close()
    conn.close()

    if not vysledky:
        raise HTTPException(status_code=404, detail="Nic nenalezeno.")

    response_data = []
    for row in vysledky:
        # Vzdálenost převedeme na procenta shody (jen pro hezký výstup)
        # 1 - vzdálenost = shoda (např. 1 - 0.4 = 0.6, tedy 60%)
        shoda_procenta = round((1 - row[3]) * 100, 2)
        
        # Filtrujeme úplné nesmysly v Pythonu
        # Pokud je shoda menší než 10 %, raději to neukážeme
        if shoda_procenta < 10:
            continue

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
        "results": response_data
    }