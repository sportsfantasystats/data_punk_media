import TopDownHockey_Scraper.TopDownHockey_EliteProspects_Scraper as tdhepscrape
import pandas as pd
from unidecode import unidecode

df = tdhepscrape.get_skaters("ahl", "2025-2026")

ahl_df = pd.DataFrame(df)

ahl_df["player"] = ahl_df["player"].str.replace(r"\s*\(.*?\)", "", regex=True)

ahl_df.columns = ["PLAYER", "TEAM", "GP", "G", "A", "PTS", "PPG", "PIM", "PLUS_MIN", "LINK", "SEASON", "LEAGUE",
              "PLAYERNAME", "POSITION"]

ahl_df["GP"] = pd.to_numeric(ahl_df["GP"], errors="coerce")
ahl_df["G"] = pd.to_numeric(ahl_df["G"], errors="coerce")
ahl_df["A"] = pd.to_numeric(ahl_df["A"], errors="coerce")
ahl_df["PIM"] = pd.to_numeric(ahl_df["PIM"], errors="coerce")

ahl_df["GPG"] = round(ahl_df["G"] / ahl_df["GP"], 3)
ahl_df["APG"] = round(ahl_df["A"] / ahl_df["GP"], 3)
ahl_df["PIMPG"] = round(ahl_df["PIM"] / ahl_df["GP"], 3)

ahl_df = ahl_df[["PLAYER", "TEAM", "GP", "G", "A", "PTS", "APG", "GPG", "PPG", "PIMPG", "PLUS_MIN", "LINK", "SEASON", "LEAGUE",]]

ahl_df["PLAYER"] = ahl_df["PLAYER"].apply(unidecode)

print(ahl_df)

ahl_df.to_csv("ahl_df.csv", index=False)