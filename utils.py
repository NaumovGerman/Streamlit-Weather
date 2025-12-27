from datetime import datetime, timezone, timedelta
import time
import pandas as pd
import polars as pl
import plotly.graph_objects as go


def use_pandas(file):
    start = time.time()
    data = pd.read_csv(file, parse_dates=['timestamp'])

    data.sort_values(['city', 'timestamp'], inplace=True)

    data['rolling_mean'] = data.groupby('city')['temperature'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)

    statistics_by_city_season = data.groupby(['city', 'season'])['temperature'].agg(mean_temp='mean', std_temp='std').reset_index()

    merged = data.merge(statistics_by_city_season, on=['city', 'season'], how='left')

    merged['anomaly'] = False
    merged.loc[(merged['temperature'] > merged['mean_temp'] + 2 * merged['std_temp']) | 
            (merged['temperature'] < merged['mean_temp'] - 2 * merged['std_temp']), 'anomaly'] = True

    end = time.time()

    return merged, end - start


def use_polars(file):
    start = time.time()

    data = (
        pl.scan_csv(file, try_parse_dates=True)
        .sort(['city', 'timestamp'])
        .with_columns(
            pl.col('temperature')
                .rolling_mean(30)
                .over('city')
                .alias('rolling_mean')
        )
    )

    statistics_by_city_season = (
        data.group_by(['city', 'season'])
        .agg(
            pl.col('temperature').mean().alias('mean_temp'),
            pl.col('temperature').std().alias('std_temp')
        )
    )

    merged = (
        data.join(statistics_by_city_season, on=['city', 'season'])
        .with_columns(
            (
                (pl.col('temperature') > pl.col('mean_temp') + 2 * pl.col('std_temp')) |
                (pl.col('temperature') < pl.col('mean_temp') - 2 * pl.col('std_temp'))
            ).alias('anomaly')
        )
        .collect()
    )
    end = time.time()

    return merged, end - start


def get_season(data: dict):
    ts = data["dt"]
    tz_offset = data["timezone"]  # секунд; бывает 0 для UTC [web:26]
    utc_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    local_dt = utc_dt + timedelta(seconds=tz_offset)
    month = local_dt.month

    if month in (12, 1, 2):
        return "winter"
    elif month in (3, 4, 5):
        return "spring"
    elif month in (6, 7, 8):
        return "summer"
    else:
        return "autumn"

def get_anomalies_chart(df, target_city):
    fig = go.Figure()

    # линия температуры
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["temperature"],
            mode="lines",
            name="Температура"
        )
    )
    anomalies = df[df.anomaly]
    fig.add_trace(
        go.Scatter(
            x=anomalies["timestamp"],
            y=anomalies["temperature"],
            mode="markers",
            name="Аномалии",
            marker=dict(color="red", size=6)
        )
    )
    fig.update_layout(
        title=f"Температура в {target_city}",
        xaxis_title="Дата",
        yaxis_title="Температура (°C)",
        hovermode="x unified"
    )
    return fig

def get_season_charts(df, target_city):
    season_statistics = df.groupby("season").agg(mean_temp=("temperature", "mean"), std_temp=("temperature", "std")).reset_index()
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=season_statistics["season"],
            y=season_statistics["mean_temp"],
            name="Средняя температура"
        )
    )

    fig.add_trace(
        go.Scatter(
            x=season_statistics["season"],
            y=season_statistics["mean_temp"] + 2*season_statistics["std_temp"],
            mode="lines",
            name="+2σ",
            line=dict(dash="dash")
        )
    )

    fig.add_trace(
        go.Scatter(
            x=season_statistics["season"],
            y=season_statistics["mean_temp"] - 2*season_statistics["std_temp"],
            mode="lines",
            name="-2σ",
            line=dict(dash="dash")
        )
    )

    fig.update_layout(
        title=f"Сезонный профиль: {target_city}",
        yaxis_title="Температура (°C)"
    )
    return fig


def is_anomaly(df, city, season, temp):
    mean_temp, std = df.loc[(df.city == city) & (df.season == season),
                            ['mean_temp', 'std_temp']].iloc[0].values

    return (temp < mean_temp - 2*std) or (temp > mean_temp + 2*std)


def parse_output(out_json, df):
    if out_json['cod'] == 200:
        temperature = out_json["main"]["temp"]
        season = get_season(out_json)
        city = out_json['name']
        anomalous = is_anomaly(df, city, season, temperature)
        return {
            'code': 200,
            'temp': temperature,
            'anomalous': anomalous,
            'city': city
        }
    else:
        return {
            'code': out_json['cod'],
            'message': out_json['message'],
        }