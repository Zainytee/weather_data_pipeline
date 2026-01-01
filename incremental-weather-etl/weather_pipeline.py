from dotenv import load_dotenv
import os
import requests
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from urllib.parse import quote_plus


load_dotenv()


API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("url")            
DB_USERNAME = os.getenv("db_username")
DB_PASSWORD = os.getenv("password")
DB_NAME = os.getenv("database")
CLUSTER = os.getenv("cluster")




MONGO_URI = (
    f"mongodb+srv://{DB_USERNAME}:{quote_plus(DB_PASSWORD)}"
    f"@{CLUSTER}/{DB_NAME}"
    f"?authSource=admin&retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db["weather"]


collection.create_index([("updatedAt", 1)])
collection.create_index([("city", 1), ("dt", 1)], unique=True)




def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ts_to_iso_utc(ts: int | str) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_hourly_24(url: str) -> list[dict]:
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    payload = resp.json()

    meta = {
        "city_name": payload.get("city_name"),
        "country_code": payload.get("country_code"),
        "state_code": payload.get("state_code"),
        "lat": payload.get("lat"),
        "lon": payload.get("lon"),
    }

    data = payload.get("data", []) or []
    return [{**rec, **meta} for rec in data]


def normalize(rec: dict) -> dict:
    dt_str = rec.get("timestamp_utc") or rec.get(
        "datetime") or rec.get("ob_time")
    dt_iso = (
        (dt_str.replace(" ", "T") + "Z")
        if dt_str and not dt_str.endswith("Z")
        else dt_str
        or ts_to_iso_utc(rec.get("ts", 0))
        or now_utc_iso()
    )

    city = rec.get("city_name") or "UNKNOWN"
    wx = rec.get("weather") or {}
    desc = wx.get("description") if isinstance(wx, dict) else None

    return {
        "_id": f"{city}|{dt_iso}",
        "provider": "weatherbit",
        "city": city,
        "country": rec.get("country_code") or "",
        "state_code": rec.get("state_code") or "",
        "lat": rec.get("lat"),
        "lon": rec.get("lon"),
        "dt": dt_iso,
        "temp_c": rec.get("temp"),
        "feels_like_c": rec.get("app_temp"),
        "rh": rec.get("rh"),
        "dewpt_c": rec.get("dewpt"),
        "wind_ms": rec.get("wind_spd"),
        "wind_gust_ms": rec.get("wind_gust_spd"),
        "wind_dir_deg": rec.get("wind_dir"),
        "wind_cdir": rec.get("wind_cdir"),
        "wind_cdir_full": rec.get("wind_cdir_full"),
        "pop_pct": rec.get("pop"),
        "precip_mm": rec.get("precip"),
        "snow_mm": rec.get("snow"),
        "snow_depth_mm": rec.get("snow_depth"),
        "clouds_low_pct": rec.get("clouds_low"),
        "clouds_mid_pct": rec.get("clouds_mid"),
        "clouds_hi_pct": rec.get("clouds_hi"),
        "clouds_pct": rec.get("clouds"),
        "slp_mb": rec.get("slp"),
        "pres_mb": rec.get("pres"),
        "vis_km": rec.get("vis"),
        "uv_index": rec.get("uv"),
        "dhi_wm2": rec.get("dhi"),
        "dni_wm2": rec.get("dni"),
        "ghi_wm2": rec.get("ghi"),
        "solar_rad_wm2": rec.get("solar_rad"),
        "ozone_dobson": rec.get("ozone"),
        "conditions": desc,
        "weather_code": wx.get("code") if isinstance(wx, dict) else None,
        "weather_icon": wx.get("icon") if isinstance(wx, dict) else None,
        "pod": rec.get("pod"),
        "ingestedAt": now_utc_iso(),
        "updatedAt": now_utc_iso(),
    }


def upsert_batch(collection, docs: list[dict]) -> int:
    if not docs:
        return 0
    ops = [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True)
           for d in docs]
    res = collection.bulk_write(ops, ordered=False)
    return (res.upserted_count or 0) + (res.modified_count or 0)



def main():
    try:
        full_url = f"{BASE_URL}&key={API_KEY}"
        print(f"[info] Using Weatherbit URL: {full_url}")

        records = fetch_hourly_24(full_url)
        docs = [normalize(r) for r in records]
        changed = upsert_batch(collection, docs)

        print(f"fetched={len(records)}, upserted/modified={changed}")

    except requests.HTTPError as http_err:
        print(
            f"[error] HTTP error: {http_err} | Status code: {http_err.response.status_code}")
        print(f"[debug] Response body: {http_err.response.text}")
    except Exception as e:
        print(f"[error] {e}")


if __name__ == "__main__":
    main()
