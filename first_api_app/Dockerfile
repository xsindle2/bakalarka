FROM python:3.9-slim

# 2. Nastavíme pracovní složku uvnitř kontejneru (aby v tom byl pořádek)
WORKDIR /app

# 3. Zkopírujeme seznam ingrediencí a nainstalujeme je
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Zkopírujeme zbytek aplikace (main.py)
COPY . .

# 5. DŮLEŽITÉ: Spuštění serveru
# --host 0.0.0.0 znamená "poslouchej na všech adresách" (nutné pro Docker)
# --port 8000 je standardní port
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]