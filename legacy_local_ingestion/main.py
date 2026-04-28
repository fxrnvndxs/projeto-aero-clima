import time
from ingestion_flights import buscar_voos_regiao
from ingestion_weather import buscar_clima_aeroportos

def executar_pipeline():
    print("\n" + "="*50)
    print("=== INICIANDO PIPELINE DE INGESTÃO (NEAR REAL-TIME) ===")
    print("="*50 + "\n")
    
    # 1. Coleta Voos (OpenSky)
    print(">>> Passo 1: Coletando dados de tráfego aéreo...")
    buscar_voos_regiao()
    
    # Pequena pausa de segurança entre chamadas de APIs diferentes
    print("\n--- Aguardando 5 segundos para estabilizar conexão ---")
    time.sleep(5)
    
    # 2. Coleta Clima (OpenWeather)
    print("\n>>> Passo 2: Coletando condições climáticas nos aeroportos...")
    buscar_clima_aeroportos()
    
    print("\n" + "="*50)
    print("=== PIPELINE FINALIZADO COM SUCESSO NO AMBIENTE LOCAL ===")
    print("="*50)

if __name__ == "__main__":
    executar_pipeline()