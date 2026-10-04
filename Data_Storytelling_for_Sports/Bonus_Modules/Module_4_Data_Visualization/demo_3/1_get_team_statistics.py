import requests
import pandas as pd
from datetime import datetime

current_date = datetime.today().strftime('%Y-%m-%d')

nhl_api_url = f"https://api-web.nhle.com/v1/standings/{current_date}"

response = requests.get(nhl_api_url)

if response.status_code == 200:
    data = response.json()
else:
    print(f"Error: Unable to fetch data (Status Code {response.status_code})")
    exit()

nhl_teams_data = data.get("standings", [])

nhl_team_stats_df = pd.json_normalize(nhl_teams_data)

nhl_team_stats_df.rename(columns={
    "conferenceAbbrev": "CONF",
    "conferenceHomeSequence": "CONF_HOME",
    "conferenceL10Sequence": "CONF_L10",
    "conferenceName": "CONF_NAME",
    "conferenceRoadSequence": "CONF_ROAD",
    "conferenceSequence": "CONF_SEQ",
    "date": "DATE",
    "divisionAbbrev": "DIV_ABBR",
    "divisionHomeSequence": "DIV_HOME",
    "divisionL10Sequence": "DIV_L10",
    "divisionName":"DIV_NAME",
    "divisionRoadSequence":"DIV_ROAD",
    "divisionSequence":"DIV_SEQ",
    "gameTypeId":"GAME_TYPE_ID",
    "gamesPlayed":"GP",
    "goalDifferential":"G_DIFF",
    "goalDifferentialPctg":"G_DIFF_PCT",
    "goalAgainst":"GA",
    "goalFor":"GF",
    "goalsForPctg":"GF_PCT",
    "homeGamesPlayed":"HOME_GAMES",
    "homeGoalDifferential":"HOME_G_DIFF",
    "homeGoalsAgainst":"HOME_GA",
    "homeGoalsFor":"HOME_GF",
    "homeLosses":"HOME_LOSSES",
    "homeOtLosses":"HOME_OT_LOSSES",
    "homePoints":"HOME_PTS",
    "homeRegulationPlusOtWins":"HOME_REG_PLUS_OT_W",
    "homeRegulationWins":"HOME_REG_W",
    "homeTies":"HOME_TIES",
    "homeWins":"HOME_WINS",
    "l10GamesPlayed":"L10_GP",
    "l10GoalDifferential":"L10_G_DIFF",
    "l10GoalsAgainst":"L10_GA",
    "l10GoalsFor":"L10_GF",
    "l10Losses":"L10_L",
    "l10OtLosses":"L100T_L",
    "l10Points":"L10_PTS",
    "l10RegulationPlusOtWins":"L10_REG_P_OT_W",
    "l10RegulationWins":"L10_REG_T",
    "l10Ties":"L10_T",
    "l10Wins":"L10_W",
    "leagueHomeSequence":"LEAGUE_HOME_SEQ",
    "leagueL10Sequence":"LEAGUE_L10_SEQ",
    "leagueRoadSequence":"LEAGUE_RD_SEQ",
    "leagueSequence":"LEAGUE_SEQ",
    "losses":"L",
    "otLosses":"OT_L",
    "pointPctg":"PT_PCT",
    "points":"PTS",
    "regulationPlusOtWinPctg":"REG_PLUS_OT_W_PCT",
    "regulationPlusOtWins":"REG_PLUS_OT_W",
    "regulationWinPctg":"REG_W_PCT",
    "regulationWins":"REG_W",
    "roadGamesPlayed":"ROAD_GP",
    "roadGoalDifferential":"ROAD_G_DIFF",
    "roadGoalsAgainst":"ROAD_GA",
    "roadGoalsFor":"ROAD_GF",
    "roadLosses":"ROAD_L",
    "roadOtLosses":"ROAD_OT_L",
    "roadPoints":"ROAD_PTS",
    "roadRegulationPlusOtWins":"ROAD_REG_PLUS_OT_W",
    "roadRegulationWins":"ROAD_REG_W",
    "roadTies":"ROAD_T",
    "roadWins":"ROAD_W",
    "seasonId":"SEASON_ID",
    "shootoutLosses":"SO_L",
    "shootoutWins":"SO_W",
    "streakCode":"STREAK_CODE",
    "streakCount":"STREAK_COUNT",
    "teamLogo":"TEAM_LOGO",
    "ties":"TIES",
    "waiversSequence":"WAIV_SEQ",
    "wildcardSequence":"WILD_CARD_SEQ",
    "winPctg":"WIN_PCT",
    "wins":"W",
    "placeName.default":"CITY",
    "teamName.default":"TEAM_NAME",
    "teamName.fr":"TEAM_NAME_FR",
    "teamCommonName.default":"TEAM_COMMON_NAME",
    "teamAbbrev.default":"TEAM_ABBR",
    "placeName.fr":"PLACE_NAME_FR",
    "teamCommonName.fr":"TEAM_COMMON_NAME_FR"

}, inplace=True)

csv_file_path = "nhl_team_stats_and_standings.csv"
nhl_team_stats_df.to_csv(csv_file_path, index=False)

print(f"Team Stats & Standings file has been saved successfully: {csv_file_path}")
