"""Kontrola zápisu identifikátorů bez připojení k databázi."""

import re
from typing import Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, StrictStr


TypIdentifikatoru = Literal[
    "ico", "zuj", "lau2", "nuts3", "lau1", "ruian", "qcode",
    "geonames", "momc", "kod_cobce", "psc", "mesta_obce", "kod_ku",
]

# Pravidla přesného vstupního zápisu.
FORMATY: Dict[str, Tuple[str, str]] = {
    "kod_ku": (r"[0-9]{6}", "Kód katastrálního území musí obsahovat přesně 6 číslic; přijímají se i nuly na začátku."),
    "ico": (r"[0-9]{8}", "IČO musí obsahovat přesně 8 číslic včetně úvodních nul."),
    "lau2": (r"[0-9]{6}", "LAU2/ZÚJ musí obsahovat přesně 6 číslic; přijímají se i nuly na začátku."),
    "nuts3": (r"CZ[0-9]{3}", "NUTS3 musí mít tvar CZ a 3 číslice."),
    "lau1": (r"CZ[0-9]{3}[0-9A-Z]", "LAU1 musí mít tvar CZ, 3 číslice a číslici nebo velké písmeno."),
    "ruian": (r"[1-9][0-9]*", "Kód RÚIAN musí být kladné celé číslo bez úvodních nul; přijímají se i nuly na začátku."),
    "qcode": (r"Q[1-9][0-9]*", "QCODE musí obsahovat velké Q a kladné celé číslo bez úvodních nul."),
    "geonames": (r"[1-9][0-9]*", "GEONAMES musí být kladné celé číslo bez úvodních nul; přijímají se i nuly na začátku."),
    "momc": (r"[0-9]{6}", "MOMC musí obsahovat přesně 6 číslic; přijímají se i nuly na začátku."),
    "kod_cobce": (r"[0-9]{6}", "Kód části obce musí obsahovat přesně 6 číslic včetně úvodních nul; přijímají se i nuly na začátku."),
    "psc": (r"[0-9]{3}[ ][0-9]{2}", "PSČ musí mít tvar 123 45; přijímá se také 12345."),
    "mesta_obce": (r"0*[1-9][0-9]*", "MESTA_OBCE musí být kladné celé číslo; úvodní nuly jsou povolené."),
}


class VysledekValidace(BaseModel):
    type: str
    input_value: str
    format_valid: bool
    accepted: bool
    checksum_valid: Optional[bool] = None
    valid: bool
    errors: List[Dict[str, str]] = []


def normalizovat_zapis(typ: str, hodnota: str) -> str:
    """Odstraní běžné mezery a upraví zápis podle typu identifikátoru."""
    upravena = hodnota.replace(" ", "")

    if typ == "ico":
        if re.fullmatch(r"0*[0-9]{8}", upravena):
            upravena = upravena[-8:]
        elif re.fullmatch(r"[0-9]{1,7}", upravena):
            upravena = upravena.zfill(8)

    elif typ in ("lau2", "kod_ku"):
        if re.fullmatch(r"0*[0-9]{6}", upravena):
            upravena = upravena[-6:]

    elif typ == "nuts3":
        upravena = upravena.upper()

    elif typ == "lau1":
        upravena = upravena.upper()

    elif typ == "ruian":
        upravena = upravena.lstrip("0") or "0"

    elif typ == "qcode":
        upravena = upravena.upper()

    elif typ == "geonames":
        upravena = upravena.lstrip("0") or "0"

    elif typ == "momc":
        if re.fullmatch(r"0*[0-9]{6}", upravena):
            upravena = upravena[-6:]

    elif typ == "kod_cobce":
        if re.fullmatch(r"0*[0-9]{6}", upravena):
            upravena = upravena[-6:]
        elif re.fullmatch(r"[0-9]{1,5}", upravena):
            upravena = upravena.zfill(6)

    elif typ == "psc":
        if re.fullmatch(r"0*[0-9]{5}", upravena):
            cislice = upravena[-5:]
            upravena = cislice[:3] + " " + cislice[3:]

    return upravena


def overit_identifikator(typ: TypIdentifikatoru, hodnota: str) -> VysledekValidace:
    """Ověří zápis, u IČO i kontrolní číslici; valid nevyjadřuje existenci."""
    typ = "lau2" if typ == "zuj" else typ
    vzor, popis = FORMATY[typ]
    format_valid = re.fullmatch(vzor, hodnota) is not None
    upravena = normalizovat_zapis(typ, hodnota)
    accepted = re.fullmatch(vzor, upravena) is not None
    vysledek = VysledekValidace(
        type=typ, input_value=hodnota, format_valid=format_valid,
        accepted=accepted, valid=accepted,
    )
    if not accepted:
        vysledek.errors.append({"code": "invalid_format", "message": popis})
        return vysledek

    if typ == "ico":
        # Váhy 8 až 2 a modulo 11 odhalují překlep i u správné délky IČO
        soucet = sum(int(cislice) * vaha for cislice, vaha in zip(upravena[:7], range(8, 1, -1)))
        kontrolni_cislice = (11 - soucet % 11) % 10
        vysledek.checksum_valid = int(upravena[-1]) == kontrolni_cislice
        vysledek.valid = vysledek.checksum_valid
        if not vysledek.checksum_valid:
            vysledek.errors.append({"code": "invalid_checksum", "message": "Kontrolní číslice IČO neodpovídá."})
    return vysledek
