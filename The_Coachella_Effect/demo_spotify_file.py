import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import datetime
import time

# Section 1: Setup

CLIENT_ID = '<Add Client ID Here>'
CLIENT_SECRET = '<Add Secret Here>'

today = datetime.date.today()

auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)


ARTISTS = [
    'Chappell Roan', 'Sabrina Carpenter', 'Peso Pluma',
    'Anitta', 'Dominic Fike', 'Djo', 'Clairo', 'GloRilla'
]

# Section 2: Helper Functions

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

# Section 3: Function Call & Dataset Creation

records = []

for artist in ARTISTS:
    print(f"Fetching data for {artist} on {today}...")
    snapshot = get_artist_data_snapshot(artist, today)
    if snapshot:
        records.append(snapshot)
    time.sleep(0.5)

artist_df = pd.DataFrame(records)

output_file = f"daily_artist_metrics_{today}.csv"
try:
    existing_df = pd.read_csv(output_file)
    artist_df = pd.concat([existing_df, artist_df], ignore_index=True)
except FileNotFoundError:
    pass

artist_df.to_csv(output_file, index=False)
print(f"✅ Data saved to {output_file}")
