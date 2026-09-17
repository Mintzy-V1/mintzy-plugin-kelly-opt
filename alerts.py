import os
import requests
from datetime import datetime

from core.logging import get_logger

_alert_log = get_logger("ALERT")


class AlertManager:
    def __init__(self, token=None, chat_id=None):
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    def notify(self, message):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _alert_log.info("ALERT message=%s", message)
        if self.token and self.chat_id:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={"chat_id": self.chat_id, "text": message},
                    timeout=5
                )
            except:
                pass