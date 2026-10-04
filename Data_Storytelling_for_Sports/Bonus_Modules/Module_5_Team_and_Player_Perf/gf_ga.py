import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv("sample_gf_ga_data_v2.csv")
df = pd.DataFrame(data)

avg_df = df.groupby("TEAM", as_index=False).mean()

long_df = pd.melt(avg_df, id_vars="TEAM", value_vars=["GFPG", "GAPG"],
                  var_name="Type", value_name="Value")

long_df["Total"] = long_df.groupby("TEAM")["Value"].transform("sum")
long_df["Percent"] = long_df["Value"] / long_df["Total"]

plt.figure(figsize=(10, 6))
palette = {"GFPG": "#1f77b4", "GAPG": "#d62728"}

teams = long_df["TEAM"].unique()
for team in teams:
    team_data = long_df[long_df["TEAM"] == team]
    bottom = 0
    for _, row in team_data.iterrows():
        plt.bar(team, row["Percent"], bottom=bottom,
                color=palette[row["Type"]],
                label=row["Type"] if bottom == 0 else "")
        bottom += row["Percent"]

plt.title("Balance of Scoring vs. Defense by NHL Team (Avg GFPG vs. GAPG)", fontsize=14, weight='bold')
plt.ylabel("Percentage of Total Goals")
plt.xlabel("")
plt.xticks(rotation=45, ha='right')
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.0%}'))
plt.tight_layout()
plt.show()
