import pandas as pd
from unidecode import unidecode

player_df = pd.read_csv("nhl_player_stats_daily_snapshot.csv")
player_df["NAME"] = player_df["NAME"].apply(unidecode)
print(player_df)
player_df.to_csv("clean_nhl_player_stats_daily_snapshot.csv", index=False)

goalie_df = pd.read_csv("nhl_goalie_stats_daily_snapshot.csv")
goalie_df["NAME"] = goalie_df["NAME"].apply(unidecode)
print(goalie_df)
goalie_df.to_csv("clean_nhl_goalie_stats_daily_snapshot.csv", index=False)