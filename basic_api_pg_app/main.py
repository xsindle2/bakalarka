import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker

# 1. NASTAVENÍ DATABÁZE
# Načteme adresu z nastavení v docker-compose, nebo použijeme default
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/dbname")

# Vytvoření spojení
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 2. DEFINICE TABULKY (Model)
# Takhle říkáme: "Chci tabulku 'users', která má ID a Jméno"
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)

# Toto vytvoří tabulku v databázi, pokud ještě neexistuje (při startu)
Base.metadata.create_all(bind=engine)

# 3. FASTAPI APLIKACE
app = FastAPI()

# Pomocná třída pro validaci dat (to, co nám pošle uživatel)
class UserCreate(BaseModel):
    name: str

# --- ENDPOINTY ---

@app.get("/")
def read_root():
    return {"zprava": "Aplikace s Postgresem běží!"}

@app.post("/users/")
def create_user(user: UserCreate):
    """Vloží nové jméno do databáze"""
    db = SessionLocal()
    # Vytvoříme nového uživatele
    db_user = User(name=user.name)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    db.close()
    return db_user

@app.get("/users/")
def read_users():
    """Vypíše všechny uživatele z databáze"""
    db = SessionLocal()
    users = db.query(User).all()
    db.close()
    return users