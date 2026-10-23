"""Stažení Q-kódů a GeoNames ID českých obcí z Wikidat."""
import csv
import os
import tempfile
from typing import Optional

import requests

URL_EXPORTU = "https://query.wikidata.org/sparql"
CILOVY_SOUBOR = "/Users/jakubsindler/bakalarka/obec-finder-v1/tabulky_temp/wikidata_obce.csv"
DOTAZ = """
SELECT ?item ?lau2 ?geonames WHERE {
  ?item wdt:P7606 ?lau2.
  OPTIONAL { ?item wdt:P1566 ?geonames. }
}
"""


def stahni_q_kody() -> int:
    """Ověří výsledky dotazu, uloží CSV a vrátí počet řádků."""
    with requests.get(
        URL_EXPORTU,
        params={"format": "json", "query": DOTAZ},
        headers={"User-Agent": "MuniResolver/1.0 (Bakalarska_prace)"},
        timeout=(15, 120),
    ) as odpoved:
        odpoved.raise_for_status()
        vysledky = odpoved.json()["results"]["bindings"]

    if not vysledky:
        raise ValueError("Wikidata nevrátila žádné záznamy.")

    radky = []
    for zaznam in vysledky:
        qcode = zaznam["item"]["value"].rsplit("/", 1)[-1]
        lau2 = zaznam["lau2"]["value"]
        geonames = zaznam.get("geonames", {}).get("value", "")

        if not qcode.startswith("Q") or not qcode[1:].isdigit():
            raise ValueError(f"Wikidata vrátila neplatný Q-kód: {qcode}")
        if len(lau2) != 6 or not lau2.isdigit():
            raise ValueError(f"Wikidata vrátila neplatný kód obce: {lau2}")
        if geonames and not geonames.isdigit():
            raise ValueError(f"Wikidata vrátila neplatné GeoNames ID: {geonames}")
        radky.append([qcode, lau2, geonames])

    # Atomická výměna dat starého a nového souboru
    docasna_cesta: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="",
            dir=os.path.dirname(CILOVY_SOUBOR) or ".", delete=False,
        ) as soubor:
            docasna_cesta = soubor.name
            writer = csv.writer(soubor)
            writer.writerow(["qcode", "lau2", "geonames"])
            writer.writerows(radky)
        os.replace(docasna_cesta, CILOVY_SOUBOR)
        docasna_cesta = None
    finally:
        if docasna_cesta is not None:
            os.remove(docasna_cesta)

    return len(radky)


if __name__ == "__main__":
    try:
        pocet = stahni_q_kody()
    except Exception as chyba:
        print(f"Stažení Wikidat selhalo: {chyba}")
        raise SystemExit(1)
    print(f"Uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
