import pyspark.pandas as ps
from pyspark.sql import SparkSession, SQLContext
import os
from load_data_from_pg import psbtc_df
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

class Calc_Agg():
    def __init__(self, **kwargs):
        self.crypto_price_df = kwargs.get('crypto_data')
        self.agg_report_df = ps.DataFrame()
        pass

    # Calculate the average price of a cryptocurrency over a specified time period. 
    # This is useful for understanding the general price level or trend.
    @staticmethod
    def calc_avg(df, date_val, col):
        grouped_avg_df = df.groupby('date').sum()
        return grouped_avg_df

    # Determine the middle price in a dataset to 
    # understand the central tendency, which can be less sensitive to outliers than the mean.
    @staticmethod
    def calc_median(df, col):
        median_price = df[col].median()
        return median_price
        pass

    #Identify the most frequently occurring price in the dataset.
    #Useful in understanding the most common price level within a period.
    @staticmethod
    def calc_mode(df, col):
        mode_price = df[col].mode()
        return mode_price
        pass

    #Summarize the total volume of transactions 
    #within a certain period, which can be an indicator of market activity.
    @staticmethod
    def calc_sum(df, col):
        sum_price = df[col].sum()
        return sum_price
        pass
    
    #Identify the highest and 
    #lowest prices of a cryptocurrency to understand the price range within a period.
    @staticmethod
    def calc_max(df, col):
        max_price = df[col].max()
        return max_price
        pass

    #Identify the highest and lowest prices 
    #of a cryptocurrency to understand the price range within a period.
    @staticmethod
    def calc_min(df, col):
        min_price = df[col].min()
        return min_price
        pass
    
    #Measure the volatility of 
    #cryptocurrency prices over a certain period. Higher standard deviation indicates higher volatility.
    @staticmethod
    def calc_stddev(df, col):
        std_dev_price = df[col].std()
        return std_dev_price
        pass

    #Similar to standard deviation, it measures the 
    #dispersion of price data points. It's the square of the standard deviation.
    @staticmethod
    def calc_variance(df, col):
        variance_price = df[col].var()
        return variance_price
        pass

    def aggregate_report(self):
        self.crypto_price_df = self.crypto_price_df.astype({'price_usd': 'float'})
        print(self.crypto_price_df.groupby('date')['price_usd'].nunique())
        groupby_agg_df = self.crypto_price_df.groupby('date').agg({
        'price_usd': ['sum', 'mean', 'median', 'max', 
                    'min', 'mode', 'stddev_samp','stddev_pop', 
                    'var_samp', 'var_pop']
        })
        self.agg_report_df = groupby_agg_df.reset_index()
agg_crypto_obj = Calc_Agg(crypto_data = psbtc_df)
agg_crypto_obj.aggregate_report()
# print(agg_crypto_obj.agg_report_df.head())