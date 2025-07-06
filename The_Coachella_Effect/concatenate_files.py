import os
import pandas as pd

folder_path = "artist_data"

df_list = []

for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path)
        df_list.append(df)

combined_df = pd.concat(df_list, ignore_index=True)

combined_df.to_csv("combined_artist_data.csv", index=False)

print("All CSVs have been combined and saved to 'combined_artist_data.csv'")


