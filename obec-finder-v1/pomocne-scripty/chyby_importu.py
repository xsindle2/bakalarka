"""Rozlišení nepoužitelného zdroje od chyby cílové databáze."""


class ChybaZdrojovychDat(ValueError):
    """Zdroj nelze bezpečně importovat; původní vazby mají zůstat zachované."""
