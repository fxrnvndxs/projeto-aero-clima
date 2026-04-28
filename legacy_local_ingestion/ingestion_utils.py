import requests
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

class TokenManager:
    def __init__(self):
        self.token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        self.client_id = os.getenv("OPENSKY_CLIENT_ID")
        self.client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
        self.token = None
        self.expires_at = None

    def get_token(self):
        # Implementação da renovação automática conforme docs [cite: 79]
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        r = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        r.raise_for_status()
        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - 30)
        return self.token

    def headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}