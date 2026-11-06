"""Pravidelné spouštění aktualizace; první běh až po uplynutí intervalu."""

from datetime import datetime
import sys
import time
from zoneinfo import ZoneInfo

from aktualizovat_data import main as aktualizovat

INTERVAL_SEKUND = 24 * 60 * 60


def main() -> None:
    while True:
        cas = datetime.now(ZoneInfo("Europe/Prague")).isoformat(timespec="seconds")
        print(f"{cas} — Další aktualizace za {INTERVAL_SEKUND} sekund.", flush=True)
        time.sleep(INTERVAL_SEKUND)
        cas = datetime.now(ZoneInfo("Europe/Prague")).isoformat(timespec="seconds")
        print(f"{cas} — Spouštím plánovanou aktualizaci.", flush=True)
        try:
            aktualizovat()
        except Exception as chyba:
            cas = datetime.now(ZoneInfo("Europe/Prague")).isoformat(timespec="seconds")
            print(f"{cas} — Aktualizace selhala: {chyba}", file=sys.stderr, flush=True)
        else:
            cas = datetime.now(ZoneInfo("Europe/Prague")).isoformat(timespec="seconds")
            print(f"{cas} — Plánovaný běh dokončen.", flush=True)


if __name__ == "__main__":
    main()
