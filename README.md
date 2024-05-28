
# Stock Pricing 

## Assignment Overview

This project involves analyzing stock pricing data to understand its structure and quality. The primary goal is to assess the data's integrity, identify key features, perform necessary transformations and deploy the code smoothly to the cloud.

## Steps Taken

### 1. Understanding the Data

To begin, I loaded the stock pricing data to get an overview of its structure. The initial inspection included:
- Checking the number of rows and columns.
- Identifying any duplicates or missing values.
- Extracting key features such as the number of unique tickers and the date range of the data.

    - **Number of Rows:** 1245
    - **Number of Columns:** 7
    - **Columns:** date, open, high, low, close, volume, ticker
    - **Duplicates:** 0
    - **Missing Values:** None
    - **Number of Unique Tickers:** 5 - (AAPL, GOOG, BLK, MSFT, TSLA)
    - **Date Range:** From November 9, 2015 to November 2, 2016

### 2. Data Transformation
The data transformation process includes several steps to enrich the dataset and prepare it for analysis:
    - Date Parsing: Convert date strings to date objects, using the legacy time parser policy for compatibility.
    - Adding Calculated Columns: Add columns for previous close price, daily return, and trading frequency.
    - Aggregating Data: Calculate average daily returns, identify the most frequently traded stock, determine the most volatile stock, and find the top N days with       the highest 30-day returns.

### 3. Deployment
The final step involves deploying the transformation logic to the cloud. 
Provided in the repo the CloudFormation_template I used to deployed and test on my on environment.

