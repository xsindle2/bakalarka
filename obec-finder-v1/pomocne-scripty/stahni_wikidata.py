import requests
import csv

def stahni_q_kody():
    print("Stahuji data z Wikidat (včetně GeoNames IDs)...")
    
    # SPARQL dotaz: P7606 je LAU2, P1566 je GeoNames ID
    query = """
    SELECT ?item ?lau2 ?geonames WHERE {
      ?item wdt:P7606 ?lau2.     
      OPTIONAL { ?item wdt:P1566 ?geonames. }
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
        
        with open('../tabulky/wikidata_obce.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['qcode', 'lau2', 'geonames'])
            
            for radek in vysledky:
                qcode_url = radek['item']['value']
                qcode = qcode_url.split('/')[-1] 
                
                lau2 = radek['lau2']['value']
                
                # Ošetření, pokud GeoNames chybí
                geonames = radek['geonames']['value'] if 'geonames' in radek else ''
                
                writer.writerow([qcode, lau2, geonames])
                
        print(f"Úspěšně staženo a uloženo do 'wikidata_obce.csv'. Nalezeno záznamů: {len(vysledky)}")
    else:
        print(f"Chyba při stahování: {odpoved.status_code}")

if __name__ == "__main__":
    stahni_q_kody()