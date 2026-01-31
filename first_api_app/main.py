from fastapi import FastAPI, HTTPException

app = FastAPI()

@app.get("/")
def read_root():
    return {"zprava": "Funguje to. FastAPI běží v Dockeru."}

@app.get("/ahoj/{jmeno}")
def say_hello(jmeno: str):
    return {"zprava": f"Ahoj {jmeno}, vítej v Dockeru"}


@app.get("/hodnoceni")
def vyhodnot_skore(score: int):
    if score < 0 or score > 100:
        raise HTTPException(status_code=400, detail="Skóre musí být v intervalu 0-100")
    if score < 50:
        stav = "neuspěl"
    elif score < 80:
        stav = "splnil"
    else:
        stav = "výborný"

    return {"score": score, "stav": stav}