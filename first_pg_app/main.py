import psycopg2
import os
import time

print("--- Start ---")

# cekani na start postgresu
conn = None
while conn is None:
    try:
        print("Zkouším se připojit k databázi...")
        conn = psycopg2.connect(
            host="db",              # Název z docker-compose
            database="testdb",
            user="user",
            password="heslo123"
        )
        print("Připojeno.")
    except psycopg2.OperationalError:
        print("Databáze ještě neběží, čekám...")
        time.sleep(2)

cursor = conn.cursor()

print("Vytvářím tabulku...")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS zamestnanci (
        id SERIAL PRIMARY KEY,
        jmeno VARCHAR(50),
        role VARCHAR(50)
    );
""")

print("Vkládám data...")
cursor.execute("INSERT INTO zamestnanci (jmeno, role) VALUES ('Petr', 'Skladník');")
cursor.execute("INSERT INTO zamestnanci (jmeno, role) VALUES ('Jana', 'Manažerka');")
conn.commit()

print("Čtu data z databáze:")
cursor.execute("SELECT * FROM zamestnanci;")
vysledky = cursor.fetchall()

for radek in vysledky:
    print(f" -> ID: {radek[0]}, Jméno: {radek[1]}, Role: {radek[2]}")

cursor.close()
conn.close()
print("--- KONEC SKRIPTU ---")