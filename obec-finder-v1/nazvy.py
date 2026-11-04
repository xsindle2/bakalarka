"""Čištění názvů územních jednotek od nadbytečných prefixů."""


def vycistit_nazev(nazev_s_typem: str) -> str:
    predpony = ["Hlavní město ", "Statutární město ", "Město ", "Městys ", "Obec "]
    cisty_nazev = nazev_s_typem.strip()

    for p in predpony:
        if cisty_nazev.startswith(p):
            return cisty_nazev[len(p):]

    return cisty_nazev
