from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import logging


S3_BUCKET_NAME = 's3://aws-glue-ori-schwa-vi-home-assignment'
OUTPUT_FOLDER = 'results'
FILE_PATH='s3://aws-glue-ori-schwa-vi-home-assignment/stock_prices.csv'



# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_stock_prices(stock_prices: DataFrame, transform_date= True, date_column= 'date', date_format='MM/dd/yyyy'):
    """
    Processes stock prices DataFrame by adding previous close, daily return, and frequency columns.

    Args:
    stock_prices (DataFrame): Input DataFrame containing stock prices.
    transform_date (bool): Flag to indicate if date transformation is needed. Default is True.
    date_column (str): Name of the date column in the DataFrame. Default is 'date'.
    date_format (str): Format of the date column. Default is 'MM/dd/yyyy'.

    Returns:
    Tuple[DataFrame, Window]: Processed DataFrame with additional columns and the window specification used.
    """
    logger.info("Processing stock prices data")
    if transform_date:
        stock_prices = stock_prices.withColumn(date_column, F.to_date(stock_prices[date_column], date_format))

    window_spec = Window.partitionBy("ticker").orderBy(F.col(date_column).asc())

    stock_prices = stock_prices.withColumn("previous_close", F.lag("close").over(window_spec))
    
    stock_prices = stock_prices.withColumn(
        "daily_return",
        F.when(F.col("previous_close").isNotNull(), (F.col("close") - F.col("previous_close")) / F.col("previous_close"))
         .otherwise(F.lit(0))
    )

    stock_prices = stock_prices.withColumn(
        "previous_close",
        F.when(F.col("previous_close").isNull(), F.col("close")).otherwise(F.col("previous_close"))
    )

    stock_prices = stock_prices.withColumn("frequency", F.col("close") * F.col("volume"))

    return stock_prices, window_spec

def read_and_transform_data(file_path = FILE_PATH):
    """
    Reads stock prices data from a CSV file, processes it, and returns the transformed DataFrame.

    Args:
    file_path (str): Path to the CSV file containing stock prices data.

    Returns:
    DataFrame, Window: Transformed DataFrame with additional columns and the window specification used.
    """
    try:
        logger.info(f"Reading data from {file_path}")
        stock_prices = spark.read.csv(file_path, header=True, inferSchema=True)
        stock_prices, window_spec = process_stock_prices(stock_prices)
        logger.info("Data read and processed successfully")
        return stock_prices, window_spec
    except Exception as e:
        logger.error(f"An error occurred while reading and processing data: {e}")
        return None, None

def save_to_csv(df, s3_bucket = S3_BUCKET_NAME, filename = '',output_folder=OUTPUT_FOLDER):
    """
    Saves DataFrame to a csv file in the specified S3 bucket.

    Args:
    df (DataFrame): DataFrame to be saved.
    s3_bucket (str): S3 bucket path.
    filename (str): Filename for the csv file.
    """
    try:
        s3_path = f"{s3_bucket}/{output_folder}/{filename}"
        df.write.mode("overwrite").option("header", "true").csv(s3_path)
        logger.info(f"Data saved to {s3_path} in csv format")
    except Exception as e:
        logger.error(f"An error occurred while saving data to csv: {e}")



def average_daily_return(stock_prices):
    """
    Calculates and displays the average daily return.

    Args:
    stock_prices (DataFrame): DataFrame containing stock prices.
    """
    try:
        avg_daily_return = stock_prices.groupBy("date").agg(F.avg("daily_return").alias("average_return")).orderBy(F.col("date"))
        avg_daily_return.show()
        logger.info("Average daily return calculated and displayed")
        save_to_csv(avg_daily_return, filename="avg_daily_return")
    except Exception as e:
        logger.error(f"An error occurred while calculating average daily return: {e}")

def most_frequent_stock(stock_prices):
    """
    Finds and displays the most frequently traded stock.

    Args:
    stock_prices (DataFrame): DataFrame containing stock prices.
    """
    try:
        most_freq_stock = stock_prices.groupBy("ticker").agg(F.avg("frequency").alias("frequency")).orderBy(F.col("frequency").desc()).limit(1)
        most_freq_stock.show()
        logger.info("Most frequently traded stock calculated and displayed")
        save_to_csv(most_freq_stock, filename="most_freq_stock")
    except Exception as e:
        logger.error(f"An error occurred while calculating most frequent stock: {e}")

def most_volatile_stock(stock_prices):
    """
    Finds and displays the most volatile stock cauelated as std of daily returens multiple by sqrt of 252 that are the traded days in a year.

    Args:
    stock_prices (DataFrame): DataFrame containing stock prices.
    """
    try:
        volatility = stock_prices.groupBy("ticker").agg((F.stddev("daily_return") * F.sqrt(F.lit(252))).alias("volatility")).orderBy(F.col("volatility").desc()).limit(1)
        volatility.show()
        logger.info("Most volatile stock calculated and displayed")
        save_to_csv(volatility, filename="most_volatile_stock")
    except Exception as e:
        logger.error(f"An error occurred while calculating most volatile stock: {e}")

def top_n_days_return_dates(stock_prices, window_spec, n = 3):
    """
    Finds and displays the top N days with the highest 30-day return.
    Present top 3 date by ticker

    Args:
    stock_prices (DataFrame): DataFrame containing stock prices.
    window_spec (Window): Window specification for calculating returns.
    n (int): Number of top days to display. Default is 3.
    """
    try:
        stock_prices = stock_prices.withColumn("previous_30_close", F.lag("close", 30).over(window_spec))
        stock_prices = stock_prices.withColumn("30_day_return", (F.col("close") - F.col("previous_30_close")) / F.col("previous_30_close"))
        window_spec_desc = Window.partitionBy("ticker").orderBy(F.col("30_day_return").desc())
        stock_prices = stock_prices.withColumn("rank", F.rank().over(window_spec_desc))
        top_3_per_ticker = stock_prices.filter(F.col("rank") <= n).select("ticker", "date", "30_day_return").orderBy(F.col("ticker"))
        top_3_per_ticker.show()
        logger.info(f"Top {n} days with the highest 30-day return calculated and displayed")
        save_to_csv(top_3_per_ticker, filename="top_30_day_returns")
        save_to_csv(top_30_day_returns, filename="top_30_day_returns")
    except Exception as e:
        logger.error(f"An error occurred while calculating top {n} days with highest 30-day return: {e}")

if __name__ == "__main__":
    
    spark = SparkSession.builder \
        .appName("Stock Pricing Data Analysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
        
    stock_prices, window_spec = read_and_transform_data()
    if stock_prices is not None:
        average_daily_return(stock_prices)
        most_frequent_stock(stock_prices)
        most_volatile_stock(stock_prices)
        top_n_days_return_dates(stock_prices, window_spec)

spark.stop()
