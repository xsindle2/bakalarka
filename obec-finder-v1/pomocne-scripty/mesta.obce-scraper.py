import csv
import requests
import time
import re

def vytvor_slovnik_zuj():
    """Načte ZUJ kódy a jejich názvy."""
    zuj_dict = {}
    print("Načítám naše ZUJ kódy a názvy (LAU2 a MOMC)...")
    
    # Načtení Obcí
    try:
        with open('../tabulky/VAZ0043_0101_CS.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                zuj = row[4].lstrip('0')
                nazev = row[5].strip()
                zuj_dict[zuj] = nazev
    except Exception as e: print("Chyba čtení VAZ0043:", e)
        
    # Načtení Městských částí
    try:
        with open('../tabulky/VAZ0044_0043_CS-2.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                zuj = row[4].lstrip('0')
                nazev = row[5].strip()
                zuj_dict[zuj] = nazev
    except Exception as e: print("Chyba čtení VAZ0044:", e)
        
    return zuj_dict

def stahni_ids():
    zuj_map = vytvor_slovnik_zuj()
    print(f"Nalezeno {len(zuj_map)} unikátních uzlů k prověření.")
    print("Začínám stahovat IDs... (trva asi 1,5h)")
    
    vysledky = []
    chybejici_log = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0'
    }
    
    zpracovano = 0
    for zuj, nazev in zuj_map.items():
        zpracovano += 1
        url = f"https://mesta.obce.cz/vyhledat.asp?zuj={zuj}"
        uspech = False
        
        try:
            response = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
            shoda = re.search(r'vyhledat-([0-9]+)\.htm', response.url, re.IGNORECASE)
            
            if shoda:
                id_webu = shoda.group(1)
                vysledky.append([zuj, id_webu])
                uspech = True
                
        except Exception as e:
            pass # timeouty a pády spojení vyřešíme tím, že uspech zůstane False
        
        # není ID v adrese, nebo spadlo spojení
        if not uspech:
            chybejici_log.append(f"ZUJ: {zuj.zfill(6)} | Uzel: {nazev}")
        
        time.sleep(0.15)
        
        if zpracovano % 100 == 0:
            print(f" Zpracováno {zpracovano} z {len(zuj_map)}. Získáno: {len(vysledky)} | Nenalezeno: {len(chybejici_log)}")

    # ÚSPĚŠNE DATA (CSV)
    with open('../tabulky/mesta_obce_id.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['zuj', 'id_mesta_obce'])
        writer.writerows(vysledky)
        
    # CHYBOVÉHO LOG
    if chybejici_log:
        with open('chybejici_mesta_obce.txt', 'w', encoding='utf-8') as f_log:
            f_log.write(f"Záznamy ({len(chybejici_log)}), které nemají svůj profil na mesta.obce.cz:\n")
            f_log.write("-" * 80 + "\n")
            for chyba in chybejici_log:
                f_log.write(chyba + "\n")

    print("\n--- REPORT ---")
    print(f"Úspěšně získáno a uloženo do CSV: {len(vysledky)} IDs.")
    print(f"Nepodařilo se nalézt: {len(chybejici_log)} uzlů (seznam uložen do 'chybejici_mesta_obce.txt').")

if __name__ == "__main__":
    stahni_ids()