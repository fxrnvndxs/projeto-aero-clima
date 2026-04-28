import os
from dotenv import load_dotenv
from ingestion_utils import TokenManager # Agora ele importa do arquivo acima

load_dotenv()

def validar_setup():
    print("--- Iniciando Validação de Credenciais ---")
    
    # Teste 1: OpenSky
    try:
        manager = TokenManager()
        token = manager.get_token()
        print(f"✅ OpenSky: Autenticação bem-sucedida! Token gerado.")
    except Exception as e:
        print(f"❌ OpenSky: Erro na autenticação. Detalhe: {e}")

    # Teste 2: OpenWeather
    key = os.getenv("OPENWEATHER_API_KEY")
    if key and len(key) == 32:
        print(f"✅ OpenWeather: Chave detectada com sucesso.")
    else:
        print(f"❌ OpenWeather: Chave inválida no .env.")

if __name__ == "__main__":
    validar_setup()