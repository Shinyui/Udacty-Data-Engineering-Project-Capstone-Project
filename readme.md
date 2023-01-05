# Shinyui Chu Data Engineering Capstone Project
### Project Overview
In this project the EUR to USD Forex Data from 2002 to 2019 (In both hours and minute) was given.
Along with the new data to help end-user to analyze the relationship between the sentiment of news and the price.
 
### Data Dictionary
    eurusd_hour.csv data dict
    - date
    - hour
    - bid_open
    - bid_high
    - bid_low
    - bid_close
    - bid_change
    - ask_open
    - ask_high
    - ask_low
    - ask_close
    - ask_change
    
    eurusd_minute.csv data dict
    - date
    - minute
    - bid_open
    - bid_high
    - bid_low
    - bid_close
    - bid_change
    - ask_open
    - ask_high
    - ask_low
    - ask_close
    - ask_change
    
    eurusd_news.csv
    - date
    - title
    - article
    
### Instructions
To complete the project few steps are required to complete.

#### Step 1: Scope the Project and Gather Data
Since the given data include the EUR to USD Forex Data in every hours and every minutes with the news data,
so it is extremely important to verify the validity of the date. In addition, this project is dedicated to help
Forex analyst to analyze the price so the price should not be Null.

#### Step 2: Explore and Assess the Data
After exploring the given data set, there were no missing value as expected. However due to unknown reasons
data seem to be incomplete (Some time points were not indexed in the csv file).

#### Step 3: Define the Data Model
All three data sets were all in demoralized form so 
other than checking the validity of the data we can't do much about it.
However since the date attribute in this project is extremely important because
in news data date was the key to join with the price data so this attribute **must not be NULL**.
On the other hand since the price might also be a decisive attribute all columns that relate to **price** 
**must not be NULL as well**.

#### Step 4: Run ETL to Model the Data
After exploring the data set an ETL pipeline is created. 
The following are the steps we need to take
1. Create a S3 bucket
2. Iterate the data directly to get the file path of every data
3. Upload all files to s3 bucket, we created in step 1
4. To check the upload was successfully and valid, we need to check whether the
number of files between the data directory and the number of files on S3 bucket
is match if not the function will raise an error
5. 3 tables are created in redshift for further operation (Parallel)
6. Copy the file from S3 bucket to the redshift (Parallel)
7. Check each table's row number is greater than 0

#### Step 5: Complete Project Write Up
In the future we might want to directly query the price data and join with the news data 
to analyze the sentiment of the news and the relationship with price.
We chose S3 to be the container to dock our data since we are given csv data sets.
In addition we also picked Redshift as our db, because we can dump the data that were docked on S3 to redshift easily. 
Also airflow provides wide range of connection and tool to do the heavy lift for us.
Hence it's in our toolkit as well. In the future the data should be updated real time because the price that is non-stop.

If the data was increased by 100x, I will certainly need to perform data partition.

Also if the pipelines were run on a daily basis by 7am, I will also need to schedule the pipeline to automate the work.

On the other hand, if the database needed to be accessed by 100+ people, data partition is certainly
consider a must since not all people need the "Whole" data. 