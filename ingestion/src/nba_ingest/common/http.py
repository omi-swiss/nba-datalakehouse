import requests

def get_json(url, params=None, headers=None):
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r
