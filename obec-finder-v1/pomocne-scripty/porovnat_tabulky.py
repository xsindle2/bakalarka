"""Porovnání místních původních a nově stažených CSV bez jejich změny."""

import csv
import json
from pathlib import Path
import sys
from typing import Dict, List, Tuple

SLOZKA_PUVODNI = Path(__file__).resolve().parent.parent / "tabulky"
SLOZKA_NOVE = Path(__file__).resolve().parent.parent / "tabulky_nove"
# Soubor: (používané sloupce, oddělovač, kódování).
SOUBORY = {
    "CIS0100_CS.csv": (("chodnota", "text", "cznuts", "kod_ruian"), ",", "utf-8"),
    "CIS0101_CS.csv": (("chodnota", "text", "cznuts", "okres_lau", "kod_ruian"), ",", "utf-8"),
    "VAZ0043_0101_CS.csv": (("chodnota1", "text1", "chodnota2"), ",", "utf-8"),
    "VAZ0044_0043_CS.csv": (("chodnota1", "text1", "chodnota2"), ",", "utf-8"),
    "VAZ0042_0043_CS.csv": (("chodnota1", "text1", "chodnota2"), ",", "utf-8"),
    "ico_obce.csv": (("ico", "kod_obce"), ";", "utf-8"),
    "ico_mestske_casti.csv": (("ico", "nazev"), ";", "utf-8"),
    "wikidata_obce.csv": (("qcode", "lau2", "geonames"), ",", "utf-8"),
    "zv_cobce_psc.csv": (("psc", "kodobce", "kodmomc", "kodcobce"), ";", "windows-1250"),
    "mesta_obce_id.csv": (("zuj", "id_mesta_obce"), ";", "utf-8"),
}
# Stejná pole jako v downloaderech; IČO, NUTS3, LAU1 a Wiki Q-code nuly zachovávají.
# U ostatních polí dowloadery odstraní úvodní nuly.
KODY_BEZ_UVODNICH_NUL = {
    "CIS0100_CS.csv": ("chodnota", "kod_ruian"),
    "CIS0101_CS.csv": ("chodnota", "kod_ruian"),
    "VAZ0043_0101_CS.csv": ("chodnota1", "chodnota2"),
    "VAZ0044_0043_CS.csv": ("chodnota1", "chodnota2"),
    "VAZ0042_0043_CS.csv": ("chodnota1", "chodnota2"),
    "ico_obce.csv": ("kod_obce",),
    "wikidata_obce.csv": ("lau2", "geonames"),
    "zv_cobce_psc.csv": ("psc", "kodobce", "kodmomc", "kodcobce"),
    "mesta_obce_id.csv": ("zuj", "id_mesta_obce"),
}


def nacist_pouzivane_hodnoty(soubor: Path) -> List[Tuple[str, ...]]:
    """Vybere hodnoty používané importy a seřadí záznamy se zachováním duplicit."""
    sloupce, oddelovac, kodovani = SOUBORY[soubor.name]
    radky = []
    with soubor.open(encoding=kodovani, newline="") as f:
        reader = csv.DictReader(f, delimiter=oddelovac, strict=True)
        chybejici = set(sloupce) - set(reader.fieldnames or [])
        if chybejici:
            raise ValueError(f"{soubor}: chybějící sloupce: {', '.join(sorted(chybejici))}")
        for row in reader:
            hodnoty = {pole: (row.get(pole) or "").strip() for pole in sloupce}
            if soubor.name in ("CIS0100_CS.csv", "CIS0101_CS.csv") and hodnoty["text"] == "Extra-Regio":
                continue
            if soubor.name == "VAZ0043_0101_CS.csv":
                # Stejné čištění názvu obce jako main.vycistit_nazev.
                for predpona in ("Hlavní město ", "Statutární město ", "Město ", "Městys ", "Obec "):
                    if hodnoty["text1"].startswith(predpona):
                        hodnoty["text1"] = hodnoty["text1"][len(predpona):]
                        break
            if soubor.name == "zv_cobce_psc.csv":
                hodnoty["psc"] = hodnoty["psc"].replace(" ", "")
            if soubor.name == "wikidata_obce.csv" and hodnoty["qcode"].startswith("q"):
                hodnoty["qcode"] = "Q" + hodnoty["qcode"][1:]
            for pole in KODY_BEZ_UVODNICH_NUL.get(soubor.name, ()):
                hodnoty[pole] = hodnoty[pole].lstrip("0")
            radky.append(tuple(hodnoty[pole] for pole in sloupce))
    return sorted(radky)


def porovnat_tabulky(slozka_puvodni: Path, slozka_nove: Path) -> Dict[str, List[str]]:
    """Porovná používané hodnoty CSV bez závislosti na pořadí řádků a sloupců."""
    vysledek = {
        "zmenene": [], "nezmenene": [],
        "chybejici_puvodni": [], "chybejici_nove": [],
    }
    for nazev in SOUBORY:
        puvodni = slozka_puvodni / nazev
        novy = slozka_nove / nazev
        if not puvodni.is_file():
            vysledek["chybejici_puvodni"].append(nazev)
        if not novy.is_file():
            vysledek["chybejici_nove"].append(nazev)
        if puvodni.is_file() and novy.is_file():
            if nacist_pouzivane_hodnoty(puvodni) == nacist_pouzivane_hodnoty(novy):
                vysledek["nezmenene"].append(nazev)
            else:
                vysledek["zmenene"].append(nazev)
    return vysledek


def main() -> None:
    vysledek = porovnat_tabulky(SLOZKA_PUVODNI, SLOZKA_NOVE)
    print(json.dumps(vysledek, ensure_ascii=False, indent=2))
    if vysledek["chybejici_puvodni"] or vysledek["chybejici_nove"]:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as chyba:
        print(f"Porovnání tabulek selhalo: {chyba}", file=sys.stderr)
        raise SystemExit(1)
