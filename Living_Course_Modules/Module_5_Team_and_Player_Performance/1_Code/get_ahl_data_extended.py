import TopDownHockey_Scraper.TopDownHockey_EliteProspects_Scraper as tdhepscrape
import pandas as pd
import glob
import os
from unidecode import unidecode
from datetime import datetime

# --- Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TREND_DIR = os.path.join(SCRIPT_DIR, "daily_trend_files")
SNAPSHOT_DIR = os.path.join(SCRIPT_DIR, "daily_snapshot_files")
os.makedirs(TREND_DIR, exist_ok=True)
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

# --- Get today's date ---
today = datetime.today()
date_column_format = today.strftime("%m/%d/%Y")     # 03/22/2026
date_file_format = today.strftime("%m%d%Y")         # 03222026

# --- Pull data ---
df = tdhepscrape.get_skaters("ahl", "2025-2026")

ahl_df = pd.DataFrame(df)

# --- Clean player names ---
ahl_df["player"] = ahl_df["player"].str.replace(r"\s*\(.*?\)", "", regex=True)

ahl_df = ahl_df.replace("-", 0)
ahl_df = ahl_df.fillna(0)

# --- Rename columns ---
ahl_df.columns = [
    "PLAYER", "TEAM", "GP", "G", "A", "PTS", "PPG", "PIM",
    "PLUS_MIN", "LINK", "SEASON", "LEAGUE", "PLAYERNAME", "POSITION"
]

# --- Convert numeric columns ---
ahl_df["GP"] = pd.to_numeric(ahl_df["GP"], errors="coerce")
ahl_df["G"] = pd.to_numeric(ahl_df["G"], errors="coerce")
ahl_df["A"] = pd.to_numeric(ahl_df["A"], errors="coerce")
ahl_df["PIM"] = pd.to_numeric(ahl_df["PIM"], errors="coerce")

# --- Create rate stats ---
ahl_df["GPG"] = round(ahl_df["G"] / ahl_df["GP"], 3)
ahl_df["APG"] = round(ahl_df["A"] / ahl_df["GP"], 3)
ahl_df["PIMPG"] = round(ahl_df["PIM"] / ahl_df["GP"], 3)

# --- Add DATE column ---
ahl_df["DATE"] = date_column_format

# --- Select final columns (include DATE) ---
ahl_df = ahl_df[
    ["DATE", "PLAYER", "TEAM", "GP", "G", "A", "PTS",
     "APG", "GPG", "PPG", "PIMPG", "PLUS_MIN",
     "LINK", "SEASON", "LEAGUE"]
]

# --- Normalize player names ---
ahl_df["PLAYER"] = ahl_df["PLAYER"].apply(unidecode)

ahl_df = ahl_df[ahl_df["TEAM"] != "totals"]

# --- Save daily trend file ---
trend_file = os.path.join(TREND_DIR, f"ahl_team_stats_{date_file_format}.csv")
ahl_df.to_csv(trend_file, index=False)
print(f"Trend file saved: {trend_file}")

# --- Consolidate all trend files into a single snapshot ---
trend_files = glob.glob(os.path.join(TREND_DIR, "ahl_team_stats_*.csv"))
consolidated_df = pd.concat((pd.read_csv(f) for f in trend_files), ignore_index=True)
snapshot_file = os.path.join(SNAPSHOT_DIR, "ahl_team_stats_all.csv")
consolidated_df.to_csv(snapshot_file, index=False)
print(f"Consolidated snapshot saved: {snapshot_file} ({len(consolidated_df)} rows)")