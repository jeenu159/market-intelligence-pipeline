import requests
import pandas as pd

url = "https://newsapi.org/v2/everything?q=AI&apiKey=515476f81db54e839dab597ef45677b7"

data = requests.get(url).json()

df = pd.DataFrame(data['articles'])

df['publishedAt'] = pd.to_datetime(df["publishedAt"])
df = df.dropna(subset=["title","description"])
df["source_name"] = df["source"].apply(lambda x: x['name'] if isinstance(x,dict)else None)
df = df.drop(columns=["source"])

print(f"saved {len(df)} clean articles to market_data.csv")
df.to_csv("market_data.csv", index= False)