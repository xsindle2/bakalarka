import time
import csv
import os
import psycopg2
from fastapi import FastAPI, HTTPException
from typing import List, Dict
import Levenshtein

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
    for _ in range(10):
        try:
            conn = get_db_connection()
            print("Připojeno k db.")
            break
        except psycopg2.OperationalError:
            print("db startuje, čekám...")
            time.sleep(2)
    
    if not conn: return

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS obce (
            id SERIAL PRIMARY KEY,
            lau2 VARCHAR(20) UNIQUE,
            nazev VARCHAR(100)
        );
    """)
    conn.commit()

    cursor.execute("SELECT count(*) FROM obce;")
    if cursor.fetchone()[0] == 0:
        print("Nahrávám data z zuj-name.csv")
        try:
            with open('zuj-name.csv', 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                for radek in reader:
                    cursor.execute(
                        "INSERT INTO obce (lau2, nazev) VALUES (%s, %s)",
                        (radek[3], radek[5])
                    )
                    print(radek[3], radek[5])
            conn.commit()
            print("Data nahrána.")
        except FileNotFoundError:
            print("Chyba: obce.csv nenalezen.")
    
    cursor.close()
    conn.close()


@app.get("/city/{code_input}", response_model=List[Dict[str, str]])

def find_smart_city(code_input: str):
    conn = get_db_connection()
    cursor = conn.cursor()

    # exact fit search
    cursor.execute("SELECT lau2, nazev FROM obce WHERE lau2 = %s", (code_input,))
    presna_shoda = cursor.fetchone()

    if presna_shoda:
        cursor.close()
        conn.close()
        return [{"lau2": presna_shoda[0], "mesto": presna_shoda[1]}]

    # fuzzy search
    cursor.execute("SELECT lau2, nazev FROM obce")
    vsechny_obce = cursor.fetchall()
    
    cursor.close()
    conn.close()


    kandidati = []

    for radek in vsechny_obce:
        db_code = radek[0]
        db_nazev = radek[1]
        
        vzdalenost = Levenshtein.distance(code_input, db_code)
        
        if vzdalenost <= 3:
            kandidati.append({
                "lau2": db_code,
                "mesto": db_nazev,
                "dist": vzdalenost
            })

    if not kandidati:
        raise HTTPException(status_code=404, detail="Nenalezeny žádné podobné kódy.")

    kandidati_serazeni = sorted(kandidati, key=lambda x: x["dist"])

    vysledek = []
    for k in kandidati_serazeni[:5]:
        vysledek.append({"lau2": k["lau2"], "mesto": k["mesto"]})

    return vysledek