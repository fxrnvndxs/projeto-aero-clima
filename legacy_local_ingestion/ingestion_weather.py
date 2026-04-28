import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def buscar_clima_aeroportos():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    
    # Dicionário com as coordenadas dos aeroportos focais
    aeroportos = {
        "POA": {"lat": -29.99, "lon": -51.17},
        "GRU": {"lat": -23.43, "lon": -46.47},
        "CGH": {"lat": -23.62, "lon": -46.65}
    }
    
    url = "https://api.openweathermap.org/data/2.5/weather"
    clima_atual = {}

    print(f"[{datetime.now()}] Coletando condições climáticas...")

    for iata, coord in aeroportos.items():
        params = {
            "lat": coord["lat"],
            "lon": coord["lon"],
            "appid": api_key,
            "units": "metric", # Para pegarmos Celsius e m/s
            "lang": "pt_br"
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            clima_atual[iata] = response.json()
            print(f"✅ Clima para {iata} capturado.")
        except Exception as e:
            print(f"❌ Erro ao buscar clima de {iata}: {e}")

    # Salva na Bronze
    if clima_atual:
        os.makedirs("data/bronze", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/bronze/weather_{timestamp}.json"
        
        with open(filename, "w") as f:
            json.dump(clima_atual, f, indent=4)
        
        print(f"📂 Clima salvo em: {filename}")

if __name__ == "__main__":
    buscar_clima_aeroportos()