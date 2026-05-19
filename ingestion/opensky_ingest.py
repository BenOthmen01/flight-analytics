import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timezone

load_dotenv()

# ── 1. Authentification OpenSky OAuth2 ──────────────────────────────────────

def get_opensky_token():
    """Récupère un token OAuth2 OpenSky (valable 30 min)."""
    url = (
        "https://auth.opensky-network.org/auth/realms/opensky-network"
        "/protocol/openid-connect/token"
    )
    resp = requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     os.getenv("OPENSKY_CLIENT_ID"),
        "client_secret": os.getenv("OPENSKY_CLIENT_SECRET"),
    })
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("✅ Token OpenSky obtenu")
    return token


# ── 2. Récupération des vols en temps réel ───────────────────────────────────

def fetch_flights(token: str) -> pd.DataFrame:
    """
    Récupère tous les state vectors actuels (position de chaque avion).
    Bounding box = Europe occidentale pour limiter le volume.
    """
    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamin": 35.0,   # latitude min  (Afrique du Nord)
        "lamax": 72.0,   # latitude max  (Scandinavie)
        "lomin": -25.0,  # longitude min (Atlantique)
        "lomax": 45.0,   # longitude max (Europe de l'Est)
    }
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()

    data   = resp.json()
    states = data.get("states", [])

    if not states:
        print("⚠️  Aucun vol retourné par l'API")
        return pd.DataFrame()

    columns = [
        "icao24", "callsign", "origin_country",
        "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors",
        "geo_altitude", "squawk", "spi",
        "position_source", "category",
    ]

    df = pd.DataFrame(states, columns=columns)

    # Nettoyage basique
    df["callsign"]       = df["callsign"].str.strip()
    df["ingested_at"]    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["baro_altitude"]  = pd.to_numeric(df["baro_altitude"],  errors="coerce")
    df["geo_altitude"]   = pd.to_numeric(df["geo_altitude"],   errors="coerce")
    df["velocity"]       = pd.to_numeric(df["velocity"],       errors="coerce")
    df["vertical_rate"]  = pd.to_numeric(df["vertical_rate"],  errors="coerce")
    df["longitude"]      = pd.to_numeric(df["longitude"],      errors="coerce")
    df["latitude"]       = pd.to_numeric(df["latitude"],       errors="coerce")

    # Supprime les colonnes non utiles pour Snowflake
    df = df.drop(columns=["sensors", "spi", "position_source", "category"], errors="ignore")

    print(f"✅ {len(df)} vols récupérés")
    return df


# ── 3. Connexion Snowflake ───────────────────────────────────────────────────

def get_snowflake_conn():
    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
    )
    print("✅ Connexion Snowflake OK")
    return conn


# ── 4. Création de la table RAW si elle n'existe pas ────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS RAW_FLIGHT_STATES (
    icao24          VARCHAR(10),
    callsign        VARCHAR(20),
    origin_country  VARCHAR(100),
    time_position   NUMBER,
    last_contact    NUMBER,
    longitude       FLOAT,
    latitude        FLOAT,
    baro_altitude   FLOAT,
    on_ground       BOOLEAN,
    velocity        FLOAT,
    true_track      FLOAT,
    vertical_rate   FLOAT,
    geo_altitude    FLOAT,
    squawk          VARCHAR(10),
    ingested_at     TIMESTAMP_NTZ
);
"""


# ── 5. Chargement dans Snowflake ─────────────────────────────────────────────

def load_to_snowflake(conn, df: pd.DataFrame):
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)

    rows = [
        (
            row["icao24"],
            row["callsign"],
            row["origin_country"],
            int(row["time_position"]) if pd.notna(row["time_position"]) else None,
            int(row["last_contact"])  if pd.notna(row["last_contact"])  else None,
            row["longitude"]    if pd.notna(row["longitude"])    else None,
            row["latitude"]     if pd.notna(row["latitude"])     else None,
            row["baro_altitude"] if pd.notna(row["baro_altitude"]) else None,
            bool(row["on_ground"]),
            row["velocity"]      if pd.notna(row["velocity"])      else None,
            row["true_track"]    if pd.notna(row["true_track"])    else None,
            row["vertical_rate"] if pd.notna(row["vertical_rate"]) else None,
            row["geo_altitude"]  if pd.notna(row["geo_altitude"])  else None,
            row["squawk"],
            row["ingested_at"],
        )
        for _, row in df.iterrows()
    ]

    cur.executemany(
        """
        INSERT INTO RAW_FLIGHT_STATES (
            icao24, callsign, origin_country,
            time_position, last_contact,
            longitude, latitude, baro_altitude,
            on_ground, velocity, true_track,
            vertical_rate, geo_altitude, squawk,
            ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows,
    )
    conn.commit()
    print(f"✅ {len(rows)} lignes chargées dans Snowflake → RAW.RAW_FLIGHT_STATES")
    cur.close()


# ── 6. Pipeline principal ────────────────────────────────────────────────────

def run_pipeline():
    print(f"\n🚀 Démarrage ingestion — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    token = get_opensky_token()
    df    = fetch_flights(token)

    if df.empty:
        print("Pipeline terminé — aucune donnée à charger.")
        return

    conn = get_snowflake_conn()
    try:
        load_to_snowflake(conn, df)
    finally:
        conn.close()

    print("🏁 Pipeline terminé avec succès\n")


if __name__ == "__main__":
    run_pipeline()