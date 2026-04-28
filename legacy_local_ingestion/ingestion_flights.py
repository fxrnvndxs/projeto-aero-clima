import requests
import json
import os
from ingestion_utils import TokenManager
from datetime import datetime

def buscar_voos_regiao():
    manager = TokenManager()
    headers = manager.headers()
    
    # Coordenadas aproximadas para cobrir a região de interesse (Sul/Sudeste)
    # Formato: [lamin, lomin, lamax, lomax]
    # Abrangendo aproximadamente o eixo POA - GRU - CGH
    params = {
        "lamin": -30.5, # Latitude sul (próximo a POA)
        "lomin": -52.0, # Longitude oeste
        "lamax": -23.0, # Latitude norte (passando SP)
        "lomax": -45.0  # Longitude leste
    }
    
    url = "https://opensky-network.org/api/states/all"
    
    print(f"[{datetime.now()}] Iniciando coleta de dados de voos...")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        voos = data.get("states", [])
        print(f"✅ Sucesso! Encontrados {len(voos)} aeronaves na região monitorada.")
        
        # Salvando o JSON bruto para a nossa Camada Bronze
        os.makedirs("data/bronze", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/bronze/flights_{timestamp}.json"
        
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
            
        print(f"📂 Dados salvos em: {filename}")
        return voos

    except Exception as e:
        print(f"❌ Erro na coleta de voos: {e}")
        return None

if __name__ == "__main__":
    buscar_voos_regiao()