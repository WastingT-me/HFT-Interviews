def import_modules():
    global pd, np, plt, datetime, timedelta, gc, signal, glob, os
    import pandas as pd
    import numpy as np
    import gc
    import signal
    import glob
    import os
    import matplotlib.pyplot as plt
    from datetime import datetime, timedelta

def clear_globals():
    """Очистка глобальных переменных"""
    keep_vars = ['import_modules', 'gc', 'columns', 'path_to_dir', 'clear_globals']
    for var in list(globals().keys()):
        if var not in keep_vars:
            del globals()[var]

    import_modules()
    pd.set_option('display.max_columns', None)

def common_data_preparation(df, task):
    df['ServerTimestamp [datatime, us]'] = pd.to_datetime(df['ServerTimestamp [datatime, us]'])

    start_time = datetime.strptime('10:00:00', '%H:%M:%S').time()
    end_time = datetime.strptime('18:40:00', '%H:%M:%S').time()
    df = df[(df['ServerTimestamp [datatime, us]'].dt.time >= start_time) &
                            (df['ServerTimestamp [datatime, us]'].dt.time <= end_time)]

    if task==1:
        df = df[(df.Mdtype == 0) | (df.Mdtype == 2)]
    else:
        df = df[df.Mdtype == 1]
        df['[price;qty;nborders] ask 3'] = df['[price;qty;nborders] ask 3'].astype(int)
    return df

def Frequency_data_preparation(Frequency_data, time):
    Frequency_data = Frequency_data['ServerTimestamp [datatime, us]']
    Frequency_data = Frequency_data.sort_values(by='ServerTimestamp [datatime, us]')
    Frequency_data.loc[:, 'TimeDiff'] = Frequency_data['ServerTimestamp [datatime, us]'].diff().dt.total_seconds()
    Frequency_data = Frequency_data[['ServerTimestamp [datatime, us]', 'TimeDiff']]
    Frequency_data = Frequency_data[Frequency_data['TimeDiff']!= 0].dropna()

    Frequency_data['Hour'] =   Frequency_data['ServerTimestamp [datatime, us]'].dt.hour
    Frequency_data['Minute'] = Frequency_data['ServerTimestamp [datatime, us]'].dt.minute
    Frequency_data['Second'] = Frequency_data['ServerTimestamp [datatime, us]'].dt.second

    Frequency_data.to_csv(f'Frequency_data_{time}.csv.gz', compression='gzip', index = False)
    return Frequency_data