# HFT-Interviews

[ <a href="https://colab.research.google.com/drive/1paPrt62ydwLv2U2eZqfcFsePI4X4WRR1?usp=sharing"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="google colab logo"></a>](https://colab.research.google.com/drive/1IUVTqVTB_czQn6QHAt5Tqi_Gmsnu8SJh?usp=sharing) **`MD_Task.ipynb`** contains a wrapper for performing operations on all datasets with addition (general frequency graph and statistics table).

[ <a href="https://colab.research.google.com/drive/1paPrt62ydwLv2U2eZqfcFsePI4X4WRR1?usp=sharing"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="google colab logo"></a>](https://colab.research.google.com/drive/1GvafpWjFYEz6WlLpTwljKfAvv6xQaaD7?usp=sharing) **`MD_task_Example.ipynb`** and  [ <a href="https://colab.research.google.com/drive/1paPrt62ydwLv2U2eZqfcFsePI4X4WRR1?usp=sharing"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="google colab logo"></a>](https://colab.research.google.com/drive/1niKRmqtrfhVyd2_9x5ejHghdJaj6mMV2?usp=sharing) **`MD_task_Example_Polars.ipynb`** provides an example of execution for a single dataset (the same as inside the wrapper) using `pandas` and `polars` respectively.

| **Time, sec** | **Data Load** | **Data Calculations** | **List Calculations** |
|---------------|---------------|-----------------------|-----------------------|
| **Polars**    | 19.47 | 5.34          | 1.76          |
| **Pandas**    | 44.96 | 13.84          | 1.70          |

The quantile vector calculation was used in List Calculations - `polars.Series.qcut` is slower. It seems that in `polars` there is no separate function for list instead of single value, so request looks like:
```python
time_quantiles = (
    Frequency_data
    .with_columns(
        pl.col("TimeDiff").qcut(
            quantiles,
            labels=[f'{i}' for i in range(1, 101)],
            allow_duplicates=True,
            include_breaks=True
        ).alias("quantile_bin")
    )
    .select("quantile_bin")  
    .unnest("quantile_bin")  
    .filter(pl.col("breakpoint") != float('inf')) 
    .select("breakpoint")  
    .unique()
).to_numpy().flatten() / 2
```

instead of `time_quantiles = Frequency_data['TimeDiff'].quantile(quantiles) / 2` in `pandas`.