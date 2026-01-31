import time
import csv
import os
import psycopg2
from fastapi import FastAPI, HTTPException

app = FastAPI()

# Funkce pro získání připojení k DB
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS")
    )

# --- START APLIKACE (Inicializace dat) ---
@app.on_event("startup")
def startup_db():
    # 1. Čekání na databázi (smyčka)
    conn = None
    for _ in range(10):
        try:
            conn = get_db_connection()
            print("Připojeno k databázi.")
            break
        except psycopg2.OperationalError:
            print("Databáze startuje, čekám...")
            time.sleep(2)
    
    if not conn:
        print("Nepodařilo se připojit k DB!")
        return

    cursor = conn.cursor()

    # Vytvoření tabulky (pokud neexistuje)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS obce (
            id SERIAL PRIMARY KEY,
            lau2 VARCHAR(20) UNIQUE,
            nazev VARCHAR(100)
        );
    """)
    conn.commit()

    # 3. Kontrola a naplnění dat z CSV
    cursor.execute("SELECT count(*) FROM obce;")
    pocet = cursor.fetchone()[0]

    if pocet == 0:
        print("Tabulka je prázdná. Nahrávám data z 'zuj-name.csv'...")
        
        try:
            # Otevřeme CSV soubor
            with open('zuj-name.csv', 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)
                
                for radek in reader:
                    kod_obce = radek[3]
                    nazev_obce = radek[5]
                    
                    # Používáme %s, aby to bylo bezpečné
                    cursor.execute(
                        "INSERT INTO obce (lau2, nazev) VALUES (%s, %s)",
                        (kod_obce, nazev_obce)
                    )
            
            conn.commit()
            print("Data úspěšně nahrána.")
            
        except FileNotFoundError:
            print("POZOR: Soubor 'zuj-name.csv' nebyl nalezen! Databáze je prázdná.")
    else:
        print(f"Data už v databázi jsou ({pocet} záznamů).")

    cursor.close()
    conn.close()


@app.get("/city/{lau2_code}")
def get_city(lau2_code: str):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT nazev FROM obce WHERE lau2 = %s;"
    cursor.execute(query, (lau2_code,))
    vysledek = cursor.fetchone()

    cursor.close()
    conn.close()

    if vysledek:
        # vysledek je n-tice (např. ('Praha',)), my chceme text uvnitř
        nazev_mesta = vysledek[0]
        return {"lau2": lau2_code, "mesto": nazev_mesta}
    else:
        raise HTTPException(status_code=404, detail="Obec s tímto kódem nenalezena")