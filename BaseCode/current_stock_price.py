import os
import requests


APP_KEY = os.environ.get("APP_KEY")
APP_SECRET = os.environ.get("APP_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")



URL_BASE = "https://openapi.koreainvestment.com:9443"
PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
URL = f"{URL_BASE}/{PATH}"


headers = {
    "Content-Type":"application/json", 
    "authorization": f"Bearer {ACCESS_TOKEN}",
    "appKey":APP_KEY,
    "appSecret":APP_SECRET,
    "tr_id":"FHKST01010100"
}


params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":"005930"
}

res = requests.get(URL, headers=headers, params=params)
print(res.json()['output']['stck_prpr'])

