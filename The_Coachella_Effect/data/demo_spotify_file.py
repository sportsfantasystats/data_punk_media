# import spotipy
# from spotipy.oauth2 import SpotifyClientCredentials
# import pandas as pd
# import datetime
# import time
# # from datetime import datetime
#
# # === SETUP ===
# CLIENT_ID = '<add client id here>'
# CLIENT_SECRET = '<add secret here>'
#
# ARTISTS = ['Chappell Roan', 'Sabrina Carpenter', 'Peso Pluma', 'Anitta', 'Dominic Fike', 'Djo',
#            'Clairo', 'GloRilla']
#
# YEARS = list(range(2024, 2025))
#
# current_date = datetime.datetime.now().strftime("%Y-%m-%d")
#
# # === AUTHENTICATION ===
# auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
# sp = spotipy.Spotify(auth_manager=auth_manager)
#
#
# # === FUNCTIONS ===
#
# def get_artist_id(artist_name):
#     result = sp.search(q=f'artist:{artist_name}', type='artist')
#     return result['artists']['items'][0] if result['artists']['items'] else None
#
#
# def get_artist_metrics(artist_obj):
#     return {
#         'artist_id': artist_obj['id'],
#         'artist_name': artist_obj['name'],
#         'followers': artist_obj['followers']['total'],
#         'popularity': artist_obj['popularity']
#     }
#
#
# def get_top_tracks_metrics(artist_id, market='US'):
#     try:
#         tracks = sp.artist_top_tracks(artist_id, country=market)['tracks']
#         return sum([t['popularity'] for t in tracks]) / len(tracks) if tracks else 0
#     except:
#         return 0
#
#
# def estimate_stream_revenue(followers, avg_popularity, payout_per_stream=0.004):
#     est_streams = followers * (avg_popularity / 100) * 2  # heuristic
#     return est_streams * payout_per_stream
#
#
# def get_artist_data_snapshot(artist_name, snapshot_date, coachella_date):
#     artist_obj = get_artist_id(artist_name)
#     if not artist_obj:
#         return None
#
#     metrics = get_artist_metrics(artist_obj)
#     avg_top_track_popularity = get_top_tracks_metrics(metrics['artist_id'])
#
#     revenue = estimate_stream_revenue(metrics['followers'], avg_top_track_popularity)
#
#     return {
#         'artist_name': metrics['artist_name'],
#         'artist_id': metrics['artist_id'],
#         'followers': metrics['followers'],
#         'popularity': metrics['popularity'],
#         'avg_top_track_popularity': avg_top_track_popularity,
#         'estimated_revenue': revenue,
#         'date': snapshot_date,
#         'year': snapshot_date.year,
#         'event': 'pre-Coachella' if snapshot_date < coachella_date else 'post-Coachella'
#     }
#
#
# # === DATA COLLECTION ===
# records = []
#
# for year in YEARS:
#     coachella_date = datetime.date(year, 4, 15)  # Approximate Coachella date per year
#     snapshot_dates = {
#         'pre-Coachella': datetime.date(year, 3, 1),
#         'post-Coachella': datetime.date(year, 5, 1)
#     }
#
#     for event, snapshot_date in snapshot_dates.items():
#         for artist in ARTISTS:
#             print(f"Fetching {event} data for {artist} in {year}...")
#             snapshot = get_artist_data_snapshot(artist, snapshot_date, coachella_date)
#             if snapshot:
#                 records.append(snapshot)
#             time.sleep(0.5)
#
# # === EXPORT ===
# df = pd.DataFrame(records)
# df.to_csv(f"coachella_artist_revenue_analysis_{current_date}.csv", index=False)
# print("✅ Data saved to 'coachella_artist_revenue_analysis.csv'")
#
# # === CORRELATION (Optional) ===
# correlation = df[['followers', 'popularity', 'avg_top_track_popularity', 'estimated_revenue']].corr()
# print("\n📊 Correlation matrix:")
# print(correlation)

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import datetime
import time

# === SETUP ===
CLIENT_ID = '42b0b8b5af9a42a58abd83e2a6769b07'
CLIENT_SECRET = '849ca57412574438954010851f42fa72'

ARTISTS = [
    'Chappell Roan', 'Sabrina Carpenter', 'Peso Pluma',
    'Anitta', 'Dominic Fike', 'Djo', 'Clairo', 'GloRilla'
]

today = datetime.date.today()

# === AUTHENTICATION ===
auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)


# === FUNCTIONS ===

def get_artist_id(artist_name):
    result = sp.search(q=f'artist:{artist_name}', type='artist')
    return result['artists']['items'][0] if result['artists']['items'] else None


def get_artist_metrics(artist_obj):
    return {
        'artist_id': artist_obj['id'],
        'artist_name': artist_obj['name'],
        'followers': artist_obj['followers']['total'],
        'popularity': artist_obj['popularity']
    }


def get_top_tracks_metrics(artist_id, market='US'):
    try:
        tracks = sp.artist_top_tracks(artist_id, country=market)['tracks']
        return sum([t['popularity'] for t in tracks]) / len(tracks) if tracks else 0
    except:
        return 0


def estimate_stream_revenue(followers, avg_popularity, payout_per_stream=0.004):
    est_streams = followers * (avg_popularity / 100) * 2  # heuristic
    return est_streams * payout_per_stream


def get_artist_data_snapshot(artist_name, snapshot_date):
    artist_obj = get_artist_id(artist_name)
    if not artist_obj:
        return None

    metrics = get_artist_metrics(artist_obj)
    avg_top_track_popularity = get_top_tracks_metrics(metrics['artist_id'])
    revenue = estimate_stream_revenue(metrics['followers'], avg_top_track_popularity)

    return {
        'artist_name': metrics['artist_name'],
        'artist_id': metrics['artist_id'],
        'followers': metrics['followers'],
        'popularity': metrics['popularity'],
        'avg_top_track_popularity': avg_top_track_popularity,
        'estimated_revenue': revenue,
        'date': snapshot_date
    }


# === DATA COLLECTION ===
records = []

for artist in ARTISTS:
    print(f"Fetching data for {artist} on {today}...")
    snapshot = get_artist_data_snapshot(artist, today)
    if snapshot:
        records.append(snapshot)
    time.sleep(0.5)

# === EXPORT ===
df = pd.DataFrame(records)

# Append to existing CSV if it exists, otherwise create
output_file = f"daily_artist_metrics_{today}.csv"
try:
    existing_df = pd.read_csv(output_file)
    df = pd.concat([existing_df, df], ignore_index=True)
except FileNotFoundError:
    pass

df.to_csv(output_file, index=False)
print(f"✅ Data saved to {output_file}")
