import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import joblib
from financial import make_financial_path

def make_aggregated_path(days: int):
    return os.path.join("data/files/aggregated/", str(days), f"aggregate.csv")

def make_scaler_path(days: int):
    return os.path.join("data/files/aggregated/", str(days), f"scaler.joblib.gz")

def aggregate(names: list[str], days: int):
    financials = {}
    for name in names:
        df = pd.read_csv(make_financial_path(name, is_raw=False))
        df["Timestamp"] = pd.to_datetime(df["Date"]).dt.normalize().astype(int).div(1000000000).astype(int)
        df[name] = df["CloseToClose"]
        df = df[["Timestamp", "CloseToClose"]]
        financials[name] = df

    directory = os.path.abspath("data/files/collected")
    files = os.listdir(directory)

    previous_collected_events = pd.DataFrame()
    previous_cutoff = pd.to_datetime("20200101")

    aggregated = []

    for index, file in enumerate(files):
        collected_events = pd.read_csv(os.path.join(directory, file))
        collected_events["Time"] = pd.to_datetime(collected_events["Time"])
        collected_events["Timestamp"] = collected_events["Time"].dt.normalize().astype(int).div(1000000000).astype(int)
        cutoff = collected_events["Time"].mean().normalize() if index != len(files) - 1 else collected_events["Time"].max().normalize()
        (previous_collected_events, collected_events) = (collected_events, pd.concat([previous_collected_events, collected_events]))
        for date in pd.date_range(previous_cutoff, cutoff):
            print(date)
            timestamp = int(date.normalize().timestamp())
            if collected_events["Timestamp"].le(timestamp).any():
                previous = timestamp - (60 * 60 * 24 * days)
                events = collected_events[collected_events["Timestamp"].between(previous, timestamp, inclusive="right")]

                data = []
                for category in range(1, 21):
                    category_events = events[events["EventBaseCode"] == category]

                    mentions = category_events["MentionsCount"].sum()
                    goldstein = category_events["MentionsCount"].mul(category_events["GoldsteinScale"].add(10)).sum() / (mentions * 20) if mentions > 0 else 0.5

                    words = category_events["WordCount"].sum()
                    positive = category_events["Positive"].sum() / words if words > 0 else 0
                    negative = category_events["Negative"].sum() / words if words > 0 else 0
                    finance =  category_events["Finance"].sum()  / words if words > 0 else 0

                    data.append(mentions)
                    data.append(goldstein)
                    data.append(positive)
                    data.append(negative)
                    data.append(finance)
                data.append(timestamp)
                aggregated.append(data)
        previous_cutoff = cutoff + pd.Timedelta(days=1)

    columns = [element for row in [[f"{i}_total_mentions", f"{i}_goldstein", f"{i}_positive", f"{i}_negative", f"{i}_finance"] for i in range(1, 21)] for element in row]
    columns.append("Timestamp")

    aggregated_df = pd.DataFrame(aggregated, columns=columns)
    aggregated_df = aggregated_df.iloc[14:]
    for name in names:
        financial = financials[name]
        aggregated_df = pd.merge(aggregated_df, financial, on="Timestamp", how="left")
        aggregated_df.columns = [*aggregated_df.columns[:-1], name]
    aggregated_df = aggregated_df.drop(columns=["Timestamp"])

    mention_columns = [f"{i}_total_mentions" for i in range(1, 21)]

    scaler = MinMaxScaler(feature_range=(0, 1))

    aggregated_df[mention_columns] = scaler.fit_transform(aggregated_df[mention_columns])

    aggregated_path = make_aggregated_path(days)
    os.makedirs(os.path.dirname(aggregated_path), exist_ok=True)
    aggregated_df.to_csv(aggregated_path, index=False)

    scaler_path = make_scaler_path(days)
    os.makedirs(os.path.dirname(scaler_path), exist_ok=True)
    joblib.dump(scaler, scaler_path)