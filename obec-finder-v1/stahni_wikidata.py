import requests
import csv

def stahni_q_kody():
    print("Stahuji data z Wikidat (může to chvilku trvat)...")
    
    # SPARQL dotaz: P7606 je oficiální vlastnost pro "Kód obce ČSÚ" (LAU2/ZUJ)
    query = """
    SELECT ?item ?lau2 WHERE {
      ?item wdt:P7606 ?lau2.     
    }
    """
    url = "https://query.wikidata.org/sparql"
    headers = {
        "User-Agent": "MuniResolver/1.0 (Bakalarska_prace)" 
    }
    
    odpoved = requests.get(url, params={'format': 'json', 'query': query}, headers=headers)
    
    if odpoved.status_code == 200:
        data = odpoved.json()
        vysledky = data['results']['bindings']
        
        with open('wikidata_obce.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['qcode', 'lau2']) # Hlavička
            
            for radek in vysledky:
                # Wikidata vrací celou URL (např. http://www.wikidata.org/entity/Q1085)
                # My chceme jen to "Qxxxxx" na konci
                qcode_url = radek['item']['value']
                qcode = qcode_url.split('/')[-1] 
                
                lau2 = radek['lau2']['value']
                
                writer.writerow([qcode, lau2])
                
        print(f"Úspěšně staženo a uloženo do 'wikidata_obce.csv'. Nalezeno obcí: {len(vysledky)}")
    else:
        print(f"Chyba při stahování: {odpoved.status_code}")

if __name__ == "__main__":
    stahni_q_kody()