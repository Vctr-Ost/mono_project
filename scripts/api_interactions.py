import requests
import time


def get_query(url, headers={}, retries=2, delay=61):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers)

            if resp.status_code == 200:
                return resp
            else:
                print(f"Спроба {attempt+1}: статус-код {resp.status_code}, очікування {delay} сек.")

        except Exception as e:
            print(f"Спроба {attempt+1}: помилка {resp.status_code}, очікування {delay} сек.")

        if attempt < retries:
            time.sleep(delay)
