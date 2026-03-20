import time
from typing import Dict

import requests


def ensure_connector(connect_rest_url: str, connector_name: str, connector_config: Dict[str, str]) -> None:
    connector_url = f"{connect_rest_url.rstrip('/')}/connectors/{connector_name}"
    list_url = f"{connect_rest_url.rstrip('/')}/connectors"
    payload = {"name": connector_name, "config": connector_config}

    while True:
        try:
            response = requests.get(connector_url, timeout=5)
            if response.status_code == 200:
                return

            if response.status_code == 404:
                create_response = requests.post(list_url, json=payload, timeout=10)
                if create_response.status_code in (200, 201, 409):
                    return

            if response.status_code not in (200, 404):
                response.raise_for_status()
        except Exception:
            pass

        time.sleep(5)
