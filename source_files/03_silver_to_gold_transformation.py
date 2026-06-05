# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import logging
# Logging will be used in Orchestration Pipeline 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Pipeline")

# COMMAND ----------

# This Function will load our transactions and merchants data from Silver layer
def load_data(path): 
    try: 
        df = spark.read.format("delta").load(path)
        return df
    
    except Exception as e:
        print(f"There was a problem in loading data: {e}") 

# COMMAND ----------

# Createing Time Based Features
def time_based_features(gold_enriched_transactions):
    try: 
        gold_enriched_transactions = gold_enriched_transactions.withColumns({
            "transaction_date": to_date(col("transaction_timestamp")),
            "transaction_hour": hour(col("transaction_timestamp")),
            "transaction_day_name": date_format(col("transaction_timestamp"), "EEEE"),
            "transaction_month": month(col("transaction_timestamp")),
            "transaction_quarter": quarter(col("transaction_timestamp")),
            "transaction_year": year(col("transaction_timestamp")),
            "transaction_week_of_year": weekofyear(col("transaction_timestamp")),
            "is_weekend": dayofweek(col("transaction_timestamp")).isin(1, 7),
            "time_segment": 
            when(col("transaction_hour").between(6, 11), "Morning")
            .when(col("transaction_hour").between(12, 16), "Afternoon")
            .when(col("transaction_hour").between(17, 21), "Evening")
            .otherwise("Night")
        })

        return gold_enriched_transactions

    except Exception as e:
        print(f"Engineering time based features failed, due to {e}")

# Creating peak hour flag for morning & evening]
def peak_hour_flagging(gold_enriched_transactions):
    try:
        gold_enriched_transactions = gold_enriched_transactions.withColumn(
            "is_peak_hour", when(col("time_segment").isin("Evening","Morning"),True).otherwise(False)
        )

        return gold_enriched_transactions
    
    except Exception as e:
        print(f"Flagging peak hour failed due to {e}")

# COMMAND ----------

# Create buckets based on transactions amount
def amount_bucketing_feature(gold_enriched_transactions):
    try: 
        gold_enriched_transactions = gold_enriched_transactions.withColumn(
            "amount_bucket",
            when(col("amount") <= 500, "Micro (0-500)")
            .when((col("amount") > 500) & (col("amount") <= 5000), "Small (501-5000)")
            .when((col("amount") > 5000) & (col("amount") <= 20000), "Medium (5001-20000)")
            .when((col("amount") > 20000) & (col("amount") <= 50000), "Large (20001-50000)")
            .when((col("amount") > 50000) & (col("amount") <= 100000), "High Value (50001-100000)")
            .otherwise("Above 1L")
        )
    
        return gold_enriched_transactions
    
    except Exception as e:
        print(f"Bucketing Amount failed: {e}")


# COMMAND ----------

# This Function will Update Metadata and do column reordering
def columns_updataion(gold_enriched_transactions):
    try:
        # Updating and adding metadata
        gold_enriched_transactions = gold_enriched_transactions.withColumns({
            "layer": lit("gold"),
            "enriched_at": current_timestamp()
        }) 

        # Reordering Columns
        gold_enriched_transactions = gold_enriched_transactions.select('transaction_id','customer_id','merchant_id','merchant_name','amount','transaction_currency','transaction_timestamp','payment_method','transaction_status','customer_city','device_type','merchant_category','merchant_city','merchant_tier','transaction_date','transaction_hour','transaction_day_name','transaction_month','transaction_quarter','transaction_year','transaction_week_of_year','is_weekend','time_segment','is_peak_hour','amount_bucket','data_source','layer','ingested_at','transformed_at','enriched_at')

        return gold_enriched_transactions
    
    except Exception as e:
        print(f"Updating Columns Failed, due to {e}")

# COMMAND ----------

# Imputing Nulls of Merchants
def impute_merchant_nulls(gold_enriched_transactions):
    try: 
        gold_enriched_transactions = gold_enriched_transactions.fillna({
            "merchant_name":"Unknown Merchant",
            "merchant_category":"Unknown Category",
            "merchant_city":"Unknown City",
            "merchant_tier":"Unknown Tier"
        })

        return gold_enriched_transactions
    
    except Exception as e:
        print(f"Imputing Nulls Falied, due to {e}")


# COMMAND ----------

# Execute the end-to-end pipeline workflow
def orchestration_pipeline(): 
    try:  
        logger.info("Silver Layer to Gold Layer Orchestration Pipeline Started....")
        transactions_df = load_data("/Volumes/workspace/default/payflowdatastorage/silver/transactions.delta") 
        merchants_df = load_data("/Volumes/workspace/default/payflowdatastorage/silver/merchants.delta")
        merchants_df = merchants_df.select('merchant_id', 'merchant_name', 'merchant_category', 'merchant_city', 'merchant_tier')
        gold_enriched_transactions = transactions_df.join(broadcast(merchants_df), "merchant_id", "left")
        col_count = len(gold_enriched_transactions.columns)
        logger.info(f"gold_enriched_transactions table created with shape: {gold_enriched_transactions.count(), col_count}")
        gold_enriched_transactions = time_based_features(gold_enriched_transactions)
        gold_enriched_transactions = peak_hour_flagging(gold_enriched_transactions)
        gold_enriched_transactions = amount_bucketing_feature(gold_enriched_transactions)
        gold_enriched_transactions = columns_updataion(gold_enriched_transactions)
        gold_enriched_transactions = impute_merchant_nulls(gold_enriched_transactions)
        logger.info(f"Gold Layer Feature Engineering Successfully Done, Total {len(gold_enriched_transactions.columns) - col_count} new columns created in gold_enriched_transactions")
        gold_enriched_transactions.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("/Volumes/workspace/default/payflowdatastorage/gold/gold_enriched_transactions.delta")
        logger.info("Data Successfully Loaded into Gold Layer as gold_enriched_transactions")
        logger.info("Silver Layer to Gold Layer Pipeline Successfully Completed.")

    except Exception as e:
        print(f"Silver Layer to Gold Layer Orchestration Pipeline Falied, due to {e}")


# COMMAND ----------

orchestration_pipeline()