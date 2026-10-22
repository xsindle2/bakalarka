# Vyhledávání územních celků

Rozpracovaný projekt bakalářské práce zaměřený na ukládání a vyhledávání
územních celků v České republice. Cílem je vytvořit API, které dokáže najít
obec, okres nebo jiný územní celek podle názvu i podle různých dostupných
identifikátorů.

> **Stav projektu:** aktivní vývojový prototyp. Projekt není dokončený ani
> určený pro produkční použití.

## Co je v projektu aktuálně hotové

- hierarchický datový model územních celků (kraj → okres → obec → část obce),
- oddělené ukládání územních uzlů a jejich identifikátorů,
- REST API postavené na FastAPI,
- přesné vyhledávání a fuzzy vyhledávání pro neúplné názvy a překlepy,
- filtrování podle typu identifikátoru a řazení výsledků podle priority,
- přidávání a mazání územních celků včetně práce s jejich hierarchií,
- import vybraných identifikátorů, například IČO, ZUJ/LAU2, LAU1, RÚIAN,
  NUTS3, PSČ, Wikidata Q-kódy a GeoNames ID.

## Technologie

- Python
- FastAPI a Uvicorn
- PostgreSQL
- Docker a Docker Compose

## Spuštění vývojové verze

Požadavky: Docker s podporou Docker Compose.

```bash
cd obec-finder-v1
docker compose up --build
```

API potom běží na <http://localhost:8000>. Dokumentace endpointů je dostupná
na <http://localhost:8000/docs>.

Hlavní endpointy aktuální verze:

- `GET /search/{query}` – vyhledávání,
- `POST /location` – přidání územního celku,
- `DELETE /location/{identifier_value}` – odstranění podle identifikátoru.

Rozhraní se ještě mění, proto není tento seznam ani API kontrakt definitivní.

## Co zbývá dokončit

- stabilizovat API a zpřesnit možnosti filtrování výsledků,
- navrhnout a nastavit vhodné priority identifikátorů,
- doplnit validaci vstupů a rozšířit automatizované testy,
- dokončit práci s datovými sadami a pokrytí všech relevantních typů územních
  celků,
- provést vyhodnocení rychlosti a přesnosti vyhledávání.

## Struktura repozitáře

- `obec-finder-v1/` – aktuální hlavní prototyp API a dat,
- `reporty/` – průběžné reporty a poznámky k vývoji,
- `basic_api_pg_app/`, `first_api_app/`, `first_pg_app/`, `zuj-name-app/` –
  starší prototypy a experimenty.

Projekt vzniká jako bakalářská práce a bude se v průběhu vývoje dále měnit.

## Stažení číselníku krajů ČSÚ

První krok automatizace aktualizuje pouze `CIS0100_CS.csv` z oficiálního
exportu ČSÚ iSMS, platného k datu spuštění:

```bash
python -m pip install -r obec-finder-v1/requirements.txt
python obec-finder-v1/pomocne-scripty/stahni_csu.py
```

Cílová cesta se nastavuje konstantou `SOUBOR_KRAJU` v kódu; výchozí hodnota
se odvozuje od umístění skriptu. Cílová složka musí existovat. Před nahrazením souboru se
ověří hlavička, počet sloupců a neprázdnost dat; při chybě stažení nebo
kontroly původní CSV zůstane zachované. Chyba ukončí skript s kódem `1`.
Aktualizace CSV nemění již naplněnou databázi. Další tabulky a zdroje budou
přidávány postupně.
