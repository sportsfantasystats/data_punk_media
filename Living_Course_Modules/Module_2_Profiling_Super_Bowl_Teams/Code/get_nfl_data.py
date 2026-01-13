
from __future__ import annotations

import numpy as np
import pandas as pd
import nflreadpy as nfl


SEASONS = list(range(2005, 2025)) 


def as_pandas(obj) -> pd.DataFrame:

    if isinstance(obj, pd.DataFrame):
        return obj

    # Polars and PyArrow typically expose to_pandas()
    if hasattr(obj, "to_pandas"):
        return obj.to_pandas()

    # Fallback: try to construct directly
    return pd.DataFrame(obj)


def safe_fill(df: pd.DataFrame, col: str, default=0) -> pd.DataFrame:
    if col not in df.columns:
        df[col] = default
    return df


def require_cols(df: pd.DataFrame, cols: list[str], df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"{df_name} is missing required columns: {missing}\n"
            f"Available columns (first 50): {list(df.columns)[:50]}"
        )


def prep_pbp(pbp: pd.DataFrame) -> pd.DataFrame:

    df = pbp.copy()

    for c in [
        "no_play", "qb_kneel", "qb_spike",
        "sack", "qb_hit",
        "interception", "fumble_lost",
        "touchdown", "first_down",
        "penalty", "penalty_yards",
    ]:
        df = safe_fill(df, c, 0)

    if "play_type" in df.columns:
        df = df.loc[df["play_type"].isin(["run", "pass"])].copy()
    else:
        raise ValueError(
            "PBP is missing 'play_type'. This pipeline expects nflverse pbp style columns."
        )

    df = df.loc[
        (df["no_play"] == 0) &
        (df["qb_kneel"] != 1) &
        (df["qb_spike"] != 1)
    ].copy()

    require_cols(df, ["season", "game_id", "posteam", "defteam", "epa"], "pbp")
    df = df.loc[df["epa"].notna()]
    df = df.loc[df["posteam"].notna() & df["defteam"].notna()].copy()

    return df


def compute_team_offense(pbp: pd.DataFrame) -> pd.DataFrame:
    df = pbp.copy()
    df["success"] = (df["epa"] > 0).astype(int)

    # Red zone: yardline_100 <= 20 (yards from opponent endzone)
    if "yardline_100" in df.columns:
        df["is_rz"] = (df["yardline_100"] <= 20).astype(int)
    else:
        df["is_rz"] = 0

    drive_keys = ["season", "game_id", "drive", "posteam"]
    require_cols(df, drive_keys, "pbp(offense)")

    rz_by_drive = (
        df.groupby(drive_keys, as_index=False)
          .agg(
              rz_trip=("is_rz", "max"),
              rz_td=("touchdown", lambda s: int((s == 1).any())),
          )
    )
    rz_by_drive["rz_td"] = np.where(rz_by_drive["rz_trip"] == 1, rz_by_drive["rz_td"], 0)

    off = (
        df.groupby(["season", "posteam"], as_index=False)
          .agg(
              offensive_plays=("epa", "size"),
              offensive_epa=("epa", "sum"),
              successful_plays=("success", "sum"),
              interceptions=("interception", "sum"),
              fumbles_lost=("fumble_lost", "sum"),
          )
          .rename(columns={"posteam": "team"})
    )
    off["offensive_turnovers"] = off["interceptions"] + off["fumbles_lost"]

    rz_team = (
        rz_by_drive.groupby(["season", "posteam"], as_index=False)
                   .agg(
                       red_zone_trips=("rz_trip", "sum"),
                       red_zone_td=("rz_td", "sum"),
                   )
                   .rename(columns={"posteam": "team"})
    )

    out = off.merge(rz_team, on=["season", "team"], how="left").fillna(
        {"red_zone_trips": 0, "red_zone_td": 0}
    )

    out["EPA_per_play_off"] = out["offensive_epa"] / out["offensive_plays"]
    out["Success_rate_off"] = out["successful_plays"] / out["offensive_plays"]
    out["Turnover_rate_off"] = out["offensive_turnovers"] / out["offensive_plays"]
    out["RedZone_TD_pct_off"] = np.where(
        out["red_zone_trips"] > 0, out["red_zone_td"] / out["red_zone_trips"], np.nan
    )

    return out


def compute_team_defense(pbp: pd.DataFrame) -> pd.DataFrame:
    df = pbp.copy()

    df["dropback"] = ((df["play_type"] == "pass") | (df["sack"] == 1)).astype(int)

    df["pressure_proxy"] = ((df["sack"] == 1) | (df["qb_hit"] == 1)).astype(int)

    df["takeaway"] = (df["interception"] + df["fumble_lost"]).astype(int)

    if "down" in df.columns:
        df["is_third_down"] = (df["down"] == 3).astype(int)
    else:
        df["is_third_down"] = 0

    df["third_down_stop"] = np.where(
        df["is_third_down"] == 1,
        1 - ((df["first_down"] == 1) | (df["touchdown"] == 1)).astype(int),
        0
    )

    out = (
        df.groupby(["season", "defteam"], as_index=False)
          .agg(
              defensive_plays=("epa", "size"),
              defensive_epa_allowed=("epa", "sum"),
              dropbacks_faced=("dropback", "sum"),
              pressures=("pressure_proxy", "sum"),
              takeaways=("takeaway", "sum"),
              third_down_plays=("is_third_down", "sum"),
              third_down_stops=("third_down_stop", "sum"),
          )
          .rename(columns={"defteam": "team"})
    )

    out["Def_EPA_per_play"] = out["defensive_epa_allowed"] / out["defensive_plays"]
    out["Pressure_rate_def"] = np.where(
        out["dropbacks_faced"] > 0, out["pressures"] / out["dropbacks_faced"], np.nan
    )
    out["Takeaway_rate_def"] = out["takeaways"] / out["defensive_plays"]
    out["Third_down_stop_rate_def"] = np.where(
        out["third_down_plays"] > 0, out["third_down_stops"] / out["third_down_plays"], np.nan
    )

    return out


def compute_situational(pbp: pd.DataFrame, schedules: pd.DataFrame) -> pd.DataFrame:
    df = pbp.copy()

    if "down" in df.columns:
        df["is_third_down"] = (df["down"] == 3).astype(int)
    else:
        df["is_third_down"] = 0

    df["third_down_conv"] = np.where(
        df["is_third_down"] == 1,
        ((df["first_down"] == 1) | (df["touchdown"] == 1)).astype(int),
        0
    )

    third = (
        df.groupby(["season", "posteam"], as_index=False)
          .agg(
              third_down_attempts=("is_third_down", "sum"),
              third_down_conversions=("third_down_conv", "sum"),
              penalties=("penalty", "sum"),
              penalty_yards=("penalty_yards", "sum"),
          )
          .rename(columns={"posteam": "team"})
    )

    third["Third_down_conv_pct"] = np.where(
        third["third_down_attempts"] > 0,
        third["third_down_conversions"] / third["third_down_attempts"],
        np.nan
    )

    sch = schedules.copy()
    require_cols(sch, ["season", "home_team", "away_team", "home_score", "away_score"], "schedules")

    home = sch[["season", "home_team", "home_score", "away_score"]].copy()
    home["team"] = home["home_team"]
    home["points_for"] = home["home_score"]
    home["points_against"] = home["away_score"]

    away = sch[["season", "away_team", "away_score", "home_score"]].copy()
    away["team"] = away["away_team"]
    away["points_for"] = away["away_score"]
    away["points_against"] = away["home_score"]

    tg = pd.concat([home, away], ignore_index=True)
    tg = tg.loc[tg["points_for"].notna() & tg["points_against"].notna()].copy()

    tg["games_played"] = 1
    tg["win"] = (tg["points_for"] > tg["points_against"]).astype(int)
    tg["score_diff_abs"] = (tg["points_for"] - tg["points_against"]).abs()
    tg["close_game"] = (tg["score_diff_abs"] <= 8).astype(int)
    tg["close_game_win"] = (tg["close_game"] * tg["win"]).astype(int)

    close = (
        tg.groupby(["season", "team"], as_index=False)
          .agg(
              games_played=("games_played", "sum"),
              close_games=("close_game", "sum"),
              close_game_wins=("close_game_win", "sum"),
          )
    )
    close["Close_game_win_pct"] = np.where(
        close["close_games"] > 0, close["close_game_wins"] / close["close_games"], np.nan
    )

    out = third.merge(close, on=["season", "team"], how="left")
    out["Penalty_yards_per_game"] = np.where(
        out["games_played"] > 0, out["penalty_yards"] / out["games_played"], np.nan
    )

    return out


def compute_primary_qb(pbp: pd.DataFrame) -> pd.DataFrame:

    df = pbp.copy()

    df["dropback"] = ((df["play_type"] == "pass") | (df["sack"] == 1)).astype(int)
    df = df.loc[df["dropback"] == 1].copy()

    require_cols(df, ["season", "posteam", "passer_player_id", "epa"], "pbp(QB)")
    df = df.loc[df["passer_player_id"].notna()].copy()

    df["pressure_faced"] = ((df["sack"] == 1) | (df["qb_hit"] == 1)).astype(int)
    df["qb_turnover"] = (df["interception"] + df["fumble_lost"]).astype(int)

    qb_usage = (
        df.groupby(["season", "posteam", "passer_player_id"], as_index=False)
          .agg(qb_dropbacks=("dropback", "sum"))
          .sort_values(["season", "posteam", "qb_dropbacks"], ascending=[True, True, False])
    )
    primary = qb_usage.groupby(["season", "posteam"], as_index=False).head(1)

    df = df.merge(primary[["season", "posteam", "passer_player_id"]],
                  on=["season", "posteam", "passer_player_id"], how="inner")

    qb = (
        df.groupby(["season", "posteam", "passer_player_id"], as_index=False)
          .agg(
              qb_epa=("epa", "sum"),
              qb_dropbacks=("dropback", "sum"),
              qb_turnovers=("qb_turnover", "sum"),
              sacks_taken=("sack", "sum"),
              pressures_faced=("pressure_faced", "sum"),
          )
          .rename(columns={"posteam": "team", "passer_player_id": "primary_qb_id"})
    )

    qb["QB_EPA_per_play"] = qb["qb_epa"] / qb["qb_dropbacks"]
    qb["QB_turnover_rate"] = qb["qb_turnovers"] / qb["qb_dropbacks"]
    qb["Pressure_to_sack_rate"] = np.where(
        qb["pressures_faced"] > 0, qb["sacks_taken"] / qb["pressures_faced"], np.nan
    )

    return qb


def build_team_season_table() -> pd.DataFrame:
    pbp_raw = nfl.load_pbp(SEASONS)
    pbp = as_pandas(pbp_raw)

    schedules_raw = nfl.load_schedules(SEASONS)
    schedules = as_pandas(schedules_raw)

    pbp = prep_pbp(pbp)

    off = compute_team_offense(pbp)
    deff = compute_team_defense(pbp)
    sit = compute_situational(pbp, schedules)
    qb = compute_primary_qb(pbp)

    team = (
        off.merge(deff, on=["season", "team"], how="inner")
           .merge(sit, on=["season", "team"], how="left")
           .merge(qb.drop(columns=["primary_qb_id"]), on=["season", "team"], how="left")
    )

    dupes = team.duplicated(subset=["season", "team"]).sum()
    if dupes:
        raise ValueError(f"Found {dupes} duplicate season-team rows; check groupby keys.")

    return team


if __name__ == "__main__":
    team_df = build_team_season_table()
    out_path = "team_season_raw_derived_REG_POST_2005_2024.csv"
    team_df.to_csv(out_path, index=False)
    print(team_df.head())
    print(f"Saved: {out_path}")
