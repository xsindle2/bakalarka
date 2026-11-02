"""Jediný databázový ukazatel na aktivní datové schéma.
"""

import psycopg2


def vytvorit_tabulku_aktivnich_dat(cursor: psycopg2.extensions.cursor) -> None:
    """Vytvoří tabulku ukazatele."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.aktivni_data (
            id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            schema_name TEXT NOT NULL
        );
    """)


def nacist_aktivni_schema(cursor: psycopg2.extensions.cursor) -> str:
    """Vrátí aktivní schéma; chybějící ukazatel je chyba konfigurace."""
    cursor.execute("SELECT schema_name FROM public.aktivni_data WHERE id = 1;")
    radek = cursor.fetchone()
    if radek is None:
        raise ValueError("Aktivní datové schéma zatím není nastavené.")
    return radek[0]


def nastavit_aktivni_schema(cursor: psycopg2.extensions.cursor, schema: str) -> None:
    """Nastaví ukazatel aktivního schématu."""
    cursor.execute("""
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname = %s AND tablename IN ('geo_locations', 'ids');
    """, (schema,))
    if {row[0] for row in cursor.fetchall()} != {"geo_locations", "ids"}:
        raise ValueError(f"Cílové schéma {schema!r} musí obsahovat geo_locations a ids.")
    cursor.execute("""
        INSERT INTO public.aktivni_data (id, schema_name) VALUES (1, %s)
        ON CONFLICT (id) DO UPDATE SET schema_name = %s;
    """, (schema, schema))
