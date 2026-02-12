FROM python:3.9-slim

pracovní složka
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

Zkopírujeme zbytek aplikace (main.py)
COPY . .

# Spuštění serveru
# --host 0.0.0.0 znamená "poslouchej na všech adresách" (nutné pro Docker)
# --port 8000 je standardní port
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]