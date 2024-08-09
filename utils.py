import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from matplotlib.dates import DateFormatter

def common_data_preparation(df):
    df['ServerTimestamp [datatime, us]'] = pd.to_datetime(df['ServerTimestamp [datatime, us]'])

    start_time = datetime.strptime('10:00:00', '%H:%M:%S').time()
    end_time = datetime.strptime('18:40:00', '%H:%M:%S').time()
    df = df[(df['ServerTimestamp [datatime, us]'].dt.time >= start_time) &
            (df['ServerTimestamp [datatime, us]'].dt.time <= end_time)]
    return df

def Frequency_data_preparation(Frequency_data, time):
    Frequency_data = Frequency_data[(Frequency_data.Mdtype == 0) | (Frequency_data.Mdtype == 2)]
    Frequency_data = Frequency_data[['ServerTimestamp [datatime, us]', 'Stream ']]
    Frequency_data = Frequency_data.sort_values(by='ServerTimestamp [datatime, us]')
    Frequency_data.loc[:, 'TimeDiff'] = Frequency_data['ServerTimestamp [datatime, us]'].diff().dt.total_seconds()
    Frequency_data = Frequency_data[['ServerTimestamp [datatime, us]', 'TimeDiff']]
    Frequency_data = Frequency_data[Frequency_data['TimeDiff']!= 0].dropna()

    Frequency_data['Hour'] =   Frequency_data['ServerTimestamp [datatime, us]'].dt.hour
    Frequency_data['Minute'] = Frequency_data['ServerTimestamp [datatime, us]'].dt.minute
    Frequency_data['Second'] = Frequency_data['ServerTimestamp [datatime, us]'].dt.second

    Frequency_data.to_csv(f'Frequency_data_{time}.csv.gz', compression='gzip', index = False)
    return Frequency_data

def create_time_range(start_time, end_time, interval_minutes):
    start_dt = datetime.strptime(start_time, '%H:%M:%S')
    end_dt = datetime.strptime(end_time, '%H:%M:%S')
    interval = timedelta(minutes=interval_minutes)
    current_time = start_dt
    times = []
    while current_time <= end_dt:
        times.append(current_time)
        current_time += interval
    return times

def frequency_plot(Frequency_data, time):
    frequency = Frequency_data.groupby(['Hour', 'Minute', 'Second']).size()

    # Converting the MultiIndex to a single string format "HH:MM:SS"
    frequency.index = pd.to_datetime(frequency.index.map(lambda x: f'{x[0]:02d}:{x[1]:02d}:{x[2]:02d}'), format='%H:%M:%S')

    # Generate the tick positions
    tick_positions = create_time_range(start_time='10:00:00', end_time='18:40:00', interval_minutes=20)

    # Plotting the frequency
    plt.figure(figsize=(15, 5))
    plt.plot(frequency.index, frequency, color='blue', linewidth=1, linestyle='-', alpha=0.8)
    plt.title(f'Частота обновлений данных ОВ за каждую секунду {time}', fontsize=16, pad=20)
    plt.xlabel('Время', fontsize=14, labelpad=15)
    plt.ylabel('Частота обновлений', fontsize=14, labelpad=15)

    # Set custom x-axis formatter to display only HH:MM:SS
    plt.gca().xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))

    # Manually set x-axis ticks
    plt.gca().set_xticks(tick_positions)

    plt.xticks(rotation=45, fontsize=10)
    plt.tight_layout()
    plt.show()

def distance_plot(Frequency_data, time):
    # Calculation of quantile values from 0.01 to 0.99
    quantiles = np.linspace(0.01, 0.99, 99)
    time_quantiles = Frequency_data['TimeDiff'].quantile(quantiles) / 2

    # Calculate the distance for each time value (using quantile values)
    distance_quantiles = time_quantiles * 10**8

    # Calculate the percentage of information loss
    loss_percent = 100 * (1 - quantiles)

    # Create labels for the X-axis: (time, information loss)
    x_labels = [f"{t:.6f} sec\n{loss:.1f}% information" for t, loss in zip(time_quantiles, loss_percent)]

    plt.figure(figsize=(40, 16))
    plt.plot(range(len(time_quantiles)), distance_quantiles, marker='o', color='blue', linewidth=2, markersize=4)
    plt.title(f'Зависимость возможного расстояния от % переданной информации {time}', fontsize=16)
    plt.xlabel('Время (секунды), Переданная информация (%)', fontsize=14)
    plt.ylabel('Расстояние (метры)', fontsize=14)
    plt.grid(True)
    plt.yscale('log')
    plt.xticks(ticks=range(len(time_quantiles)), labels=x_labels, rotation=45, ha='right', fontsize=8)

    plt.tight_layout()
    plt.show()

def trades(df, time):
    df = df[df.Mdtype == 1]
    df['[price;qty;nborders] ask 3'] = df['[price;qty;nborders] ask 3'].astype(int)

    # Определение стороны сделки: покупка (buy) или продажа (sell). '[price;qty;nborders] ask 3' - колонка с информацией по OrderSide
    df.loc[:, 'side'] = np.where(df['[price;qty;nborders] ask 3'] == 1, 'buy', 'sell')

    # Определение трейдов. Также можно по lastInBatch (колонка '[price;qty;nborders] ask 4').
    df.loc[:, 'TradeID'] = (
        (df['MarketTimestamp [epoch]'].shift(1) != df['MarketTimestamp [epoch]']) |
        (df['side'].shift(1) != df['side'])
    ).cumsum()

    df['Qty'] = df['[price;qty;nborders] ask 0'].apply(lambda x: float(x))

    df.to_csv(f'Trades_{time}.csv.gz', compression='gzip', index = False)
    return df

def statistics(trade_sizes, time):
    mean_qty = trade_sizes.mean()
    stddev_qty = trade_sizes.std()
    median_qty = trade_sizes.median()
    percentiles = trade_sizes.quantile([0.6, 0.7, 0.8, 0.9])

    percentile_pairs = [(f"{int(p*100)}%", int(v)) for p, v in percentiles.items()]

    print(f"\nСтатистики Qty трейдов {time}.")
    print("Mean:", mean_qty)
    print("StdDev:", stddev_qty)
    print("Median:", median_qty)
    print("\nPercentiles.")
    for pair in percentile_pairs:
        print(f"{pair[0]}th Percentile: {pair[1]:.2f}")

    return mean_qty, stddev_qty, median_qty

def calculate_probabilities(df, trade_sizes, threshold):
    # Определение фильтра по количеству
    mask = trade_sizes >= threshold

    # Применение фильтра к DataFrame с помощью маски, синхронизация индексов
    filtered = df[df['TradeID'].isin(trade_sizes[mask].index)].copy()

    # Сдвиг сторон сделок для сравнения с предыдущими
    filtered.loc[:, 'prev_side'] = filtered['side'].shift(1)

    # Сравнение текущей стороны с предыдущей
    filtered.loc[:, 'side_match'] = filtered['side'] == filtered['prev_side']

    # Подсчет совпадений
    matching_sides = filtered['side_match'].sum()

    # Общий размер выборки
    total_trades = len(filtered)

    # Вычисление вероятности
    probability = matching_sides / total_trades if total_trades > 0 else 0
    return probability

def statistics_and_probs(df, time):
    # Группировка по TradeID и вычисление суммарного объема (Qty)
    trade_sizes = df.groupby('TradeID')['Qty'].sum()

    mean_qty, stddev_qty, median_qty = statistics(trade_sizes, time)

    prob_mean = calculate_probabilities(df, trade_sizes, mean_qty)
    prob_median = calculate_probabilities(df, trade_sizes, median_qty)
    prob_mean_plus_stddev = calculate_probabilities(df, trade_sizes, mean_qty + stddev_qty)

    print(f"\nВероятность совпадения стороны сделки последующего трейда {time}.")
    print(f"Probability(Qty >= mean): {prob_mean:.4f}")
    print(f"Probability(Qty >= median): {prob_median:.4f}")
    print(f"Probability(Qty >= mean + stddev): {prob_mean_plus_stddev:.4f}")

def first_task(df, time):
    Frequency_data = Frequency_data_preparation(df, time)
    frequency_plot(Frequency_data, time)
    distance_plot(Frequency_data, time)

def second_task(df, time):
    trades_data = trades(df, time)
    statistics_and_probs(trades_data, time)