"""Stažení Q-kódů a GeoNames ID českých obcí z Wikidat."""
import csv
import os
from pathlib import Path
import sys
import tempfile
from typing import Optional

import requests

URL_EXPORTU = "https://query.wikidata.org/sparql"
CILOVY_SOUBOR = Path(__file__).resolve().parent.parent / "tabulky_nove" / "wikidata_obce.csv"
DOTAZ = """
SELECT ?item ?lau2 ?geonames WHERE {
  ?item wdt:P7606 ?lau2.
  OPTIONAL { ?item wdt:P1566 ?geonames. }
}
"""


def stahni_q_kody() -> int:
    """Stáhne výsledky dotazu, sjednotí zápis a uloží CSV."""
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
        qcode = zaznam["item"]["value"].strip().rsplit("/", 1)[-1].strip()
        if qcode.startswith("q"):
            qcode = "Q" + qcode[1:]
        lau2 = zaznam["lau2"]["value"].strip().lstrip("0")
        geonames = zaznam.get("geonames", {}).get("value", "").strip().lstrip("0")
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
        print(f"Stažení Wikidat selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
    print(f"Uloženo {pocet} záznamů do {CILOVY_SOUBOR}.")
