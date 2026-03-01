import csv
import json
import re

# --- 1. STATICKÁ DATA: Mapování Okresů na Kraje ---
# Toto nahrazuje chybějící data ze státní správy.
MAPA_KRAJU = {
    "Hlavní město Praha": ["Hlavní město Praha"],
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

# Rychlý reverzní slovník pro hledání Kraje podle Okresu
OKRES_DO_KRAJE = {}
for kraj, okresy in MAPA_KRAJU.items():
    for okres in okresy:
        OKRES_DO_KRAJE[okres.lower()] = kraj

def vycistit_nazev(nazev_s_typem):
    """Odstraní úřední předpony z názvu obce."""
    predpony = ["Hlavní město ", "Statutární město ", "Město ", "Městys ", "Obec "]
    cisty_nazev = nazev_s_typem.strip()
    for p in predpony:
        if cisty_nazev.startswith(p):
            return cisty_nazev[len(p):]
    return cisty_nazev

def normalizovat_okres(okres_str):
    """Sjednotí formát okresů (např. 'Brno - město' -> 'Brno-město')"""
    # Odstraní mezery kolem pomlčky
    return re.sub(r'\s*-\s*', '-', okres_str.strip())

def vytvor_master_data():
    print("Zahajuji ETL proces: Tvorba Master Data (Kraj -> Okres -> Obec)...")
    
    master_data = []
    
    # Pomocné slovníky pro rychlé vyhledávání a generování ID
    kraje_mapa = {}   # {"Jihomoravský kraj": "K_1"}
    okresy_mapa = {}  # {"Brno-město": "O_1"}
    obce_mapa = {}    # {"Brno_Brno-město": "OB_1"} # Klíč je Název_Okres kvůli duplicitám!

    id_kraj_counter = 1
    id_okres_counter = 1
    id_obec_counter = 1

    # --- KROK 1: Načtení IČO a sestavení základního stromu ---
    try:
        with open('uzemni-samosprava_obce_30-11-2025.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader) # Přeskočit hlavičku
            
            for radek in reader:
                ico = radek[0].zfill(8) # Zajistíme 8místné IČO
                nazev_original = radek[1]
                okres_original = radek[2]
                
                nazev_obce = vycistit_nazev(nazev_original)
                nazev_okresu = normalizovat_okres(okres_original)
                
                # Zjištění Kraje
                nazev_kraje = OKRES_DO_KRAJE.get(nazev_okresu.lower())
                if not nazev_kraje:
                    # Pokud se okres nenajde v mapě, vytvoříme "Neznámý kraj"
                    nazev_kraje = "Neznámý kraj"

                # 1. Zpracování Kraje (pokud ještě neexistuje)
                if nazev_kraje not in kraje_mapa:
                    kraj_id = f"K_{id_kraj_counter}"
                    kraje_mapa[nazev_kraje] = kraj_id
                    master_data.append({
                        "id": kraj_id,
                        "nazev": nazev_kraje,
                        "typ": "KRAJ",
                        "parent_id": None,
                        "ids": {}
                    })
                    id_kraj_counter += 1
                
                # 2. Zpracování Okresu (pokud ještě neexistuje)
                if nazev_okresu not in okresy_mapa:
                    okres_id = f"O_{id_okres_counter}"
                    okresy_mapa[nazev_okresu] = okres_id
                    master_data.append({
                        "id": okres_id,
                        "nazev": nazev_okresu,
                        "typ": "OKRES",
                        "parent_id": kraje_mapa[nazev_kraje],
                        "ids": {}
                    })
                    id_okres_counter += 1
                
                # 3. Zpracování Obce
                obec_id = f"OB_{id_obec_counter}"
                # Unikátní klíč pro mapování ZUJ (Název + Okres)
                klic_obce = f"{nazev_obce}_{nazev_okresu}"
                obce_mapa[klic_obce] = obec_id
                
                master_data.append({
                    "id": obec_id,
                    "nazev": nazev_obce,
                    "typ": "OBEC",
                    "parent_id": okresy_mapa[nazev_okresu],
                    "ids": {"ICO": ico}
                })
                id_obec_counter += 1
                
        print(f"IČO načteno. Vytvořeno: {len(kraje_mapa)} krajů, {len(okresy_mapa)} okresů, {id_obec_counter-1} obcí.")
        
    except FileNotFoundError:
        print("CHYBA: Soubor uzemni-samosprava_obce_30-11-2025.csv nebyl nalezen.")
        return

    # --- KROK 2: Doplnění ZUJ (LAU2) ---
    zuj_sparovano = 0
    try:
        with open('zuj-name.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f) # Tento soubor měl jako oddělovač čárku
            next(reader)
            
            for radek in reader:
                lau2 = radek[3]
                nazev_z_lau2 = radek[5]
                
                # ZDE JE HÁČEK: zuj-name.csv pravděpodobně nemá okres. 
                # Musíme hledat jen podle názvu. Vytvoříme si reverzní vyhledávání.
                # (Pro BP to stačí. Pokud byste měl okres i u ZUJ, párovalo by se to přesněji)
                
                nalezeny_uzel = None
                for node in master_data:
                    if node["typ"] == "OBEC" and node["nazev"] == nazev_z_lau2:
                        nalezeny_uzel = node
                        break # Vezmeme první shodu
                
                if nalezeny_uzel:
                    nalezeny_uzel["ids"]["LAU2"] = lau2
                    zuj_sparovano += 1

        print(f"ZUJ načteno a spárováno s obcemi: {zuj_sparovano}")
        
    except FileNotFoundError:
        print("CHYBA: Soubor zuj-name.csv nebyl nalezen. Obce budou bez LAU2 kódů.")

    # --- KROK 3: Uložení do JSON ---
    print("Ukládám data do master_geo.json ...")
    with open('master_geo.json', 'w', encoding='utf-8') as f:
        json.dump(master_data, f, ensure_ascii=False, indent=2)
        
    print("HOTOVO! Soubor master_geo.json byl úspěšně vytvořen.")

if __name__ == "__main__":
    vytvor_master_data()