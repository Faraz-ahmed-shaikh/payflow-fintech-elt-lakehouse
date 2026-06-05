# Databricks notebook source
# DBTITLE 1,Import & Setups
import requests
import json
from pyspark.sql.functions import *
import logging
# Logging will be used in Orchestration Pipeline 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Pipeline")

transaction_url = "https://raw.githubusercontent.com/Faraz-ahmed-shaikh/elt-fintech-dataset/main/transactions.json"
merchant_url = "https://raw.githubusercontent.com/Faraz-ahmed-shaikh/elt-fintech-dataset/main/merchant_metadata.json"

# COMMAND ----------

# DBTITLE 1,Fetching Function
# This Function will be used to fetch transactions & merchants data from github 
def fetch_github_data(datasetName,url): 
    try: 
        response = requests.get(url)
        response.raise_for_status()        

        # Note: Since the dataset is stored in JSON Lines (JSONL) format and Databricks serverless compute restricted direct RDD/sc.parallelize usage, records are parsed line-by-line, using Python json.loads() before converting to a Spark DataFrame.
        # For larger-scale production systems, distributed ingestion methods (Auto Loader, cloud storage, or spark.read.json()) would be preferred.
        json_data = [json.loads(line) for line in response.text.splitlines() if line.strip()]
        df = spark.createDataFrame(json_data) # Creating Spark Dataframe
        logger.info(f"{datasetName} data Fetched from Github")
        
        return df

    except Exception as e: 
        logger.error(f"fetching {datasetName} failed due to {e}")

# COMMAND ----------

# DBTITLE 1,Loading Function
# This Function load DFs into bronze layer of Databricks Lakehouse.
def load_to_delta(transactions_df, merchants_df): 
    try: 
        # First adding metadata columns
        transactions_df = transactions_df.withColumns({
            "data_source": lit("github_api"),
            "layer": lit("bronze"),
            "ingested_at": current_timestamp()
        })
        
        merchants_df = merchants_df.withColumns({
            "data_source": lit("github_api"),
            "layer": lit("bronze"),
            "ingested_at": current_timestamp()
        })
        
        # Then saving data as delta table to brozne folder
        transactions_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("/Volumes/workspace/default/payflowdatastorage/bronze/transactions.delta")
        merchants_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("/Volumes/workspace/default/payflowdatastorage/bronze/merchants.delta")
                    
    except Exception as e:
        print(f"Loading Data to Delta failed due to {e}")

# COMMAND ----------

# DBTITLE 1,orchestration function
# Execute the end-to-end pipeline workflow
def orchestration_pipeline(transaction_url, merchant_url): 
    try: 
        logger.info("Data Ingestation Orchestration pipeline started....")
        transactions_df = fetch_github_data("transactions",transaction_url)
        merchants_df = fetch_github_data("merchants",merchant_url)
        load_to_delta(transactions_df, merchants_df)
        logger.info(f"Both Transactions and Merchant Table Successfully stored to Delta Table with {transactions_df.count()} rows in transactions table and {merchants_df.count()} in merchants table ")
        logger.info("Data Ingestation Pipeline Successfully Completed.")
    
    except Exception as e:
        logger.error(f"Data Ingestation Pipeline failed due to {e}")

# COMMAND ----------

# DBTITLE 1,execution
orchestration_pipeline(transaction_url, merchant_url)