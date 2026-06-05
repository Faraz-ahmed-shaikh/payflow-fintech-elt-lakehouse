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

# This Function will load our enriched transactions data from gold layer
def load_data(path): 
    try: 
        df = spark.read.format("delta").load(path)
        return df
    
    except Exception as e:
        print(f"There was a problem in loading data: {e}") 

# COMMAND ----------

# Function Purpose: Identifies top-performing vendors and system reliability per merchant.
# Grain: Individual Merchant (ID + Tier).
# Key Metrics: Gross Merchandise Value, Success Rate, and Peak Transaction Hour.
def building_merchants_mart(df):
    try:
        merchants_mart = df.groupBy("merchant_id","merchant_name","merchant_category","merchant_city","merchant_tier").agg(
            count("transaction_id").alias("total_transactions"),
            sum(when(col("transaction_status")=="Success",1).otherwise(0)).alias("successful_transactions"),
            sum(when(col("transaction_status")=="Failed",1).otherwise(0)).alias("failed_transactions"),
            sum(when(col("transaction_status")=="Refunded",1).otherwise(0)).alias("refunded_transactions"),
            (sum(when(col("transaction_status") == "Success", 1).otherwise(0)) / count("transaction_id") * 100).alias("transaction_success_rate"),
            sum(when(col("transaction_status")=="Success",col("amount")).otherwise(0.0)).alias("total_gross_merchandise_value"),
            sum(when(col("transaction_status")=="Refunded",col("amount")).otherwise(0.0)).alias("total_refunded_amount"),
            avg("amount").alias("avg_transaction_value"),
            count_distinct("customer_id").alias("unique_customers"),
            mode("transaction_hour").alias("peak_transaction_hour")
        ).sort("merchant_id")

        return merchants_mart

    except Exception as e:
        print("Creating Merchants Mart Falied, due to {e}")

# COMMAND ----------

# Function Purpose: Builds 360-degree customer personas for targeted marketing.
# Grain: Unique Customer ID.
# Key Metrics: Favorite Payment Method, Weekend vs Weekday behavior, and Spending Tier.
def building_customers_mart(df): 
    try: 
        customers_mart = df.groupBy("customer_id").agg(
            count("transaction_id").alias("total_transactions"),
            sum(when(col("transaction_status")=="Success", col("amount")).otherwise(0.0)).alias("total_spend"),
            avg("amount").alias("avg_transaction_value"),
            mode("payment_method").alias("favorite_payment_method"),
            mode("time_segment").alias("favorite_transaction_time_bucket"),
            (sum(when(col("is_weekend") == True, 1).otherwise(0)) / count("transaction_id") * 100).alias("weekend_transaction_ratio"),
            sum(when(col("amount_bucket")=="High Value (50001-100000)",1).otherwise(0)).alias("high_value_transaction_count"),
            mode("merchant_category").alias("merchant_category_preference")
        ).sort("customer_id")

        return customers_mart
    
    except Exception as e: 
        print(f"Creating Customers Mart Falied, due to {e}")

# COMMAND ----------

# Function Purpose: Monitors payment gateway health and channel performance across time segments.
# Grain: Payment Method × Time Segment (Morning, Evening, etc.).
# Key Metrics: Channel Success Rate vs Failure Rate.
def building_payment_behavior_mart(df):
    try:
        payment_behavior_mart = df.groupBy("payment_method", "time_segment").agg(
            count("transaction_id").alias("transaction_count"),
            (sum(when(col("transaction_status") == "Success", 1).otherwise(0)) / count("transaction_id") * 100).alias("success_rate"),
            (sum(when(col("transaction_status") == "Failed", 1).otherwise(0)) / count("transaction_id") * 100).alias("failure_rate"),
            avg("amount").alias("avg_transaction_amount")
        ).sort("payment_method", "time_segment")

        return payment_behavior_mart
    
    except Exception as e:
        print(f"Creating Payment Behavior Mart Failed, due to {e}")


# COMMAND ----------

# Function Purpose: Tracks business growth, seasonality, and daily volume spikes.
# Grain: Daily Calendar Date.
# Key Metrics: Daily GMV, Unique User/Merchant growth, and Peak Hour Volume.
def building_transaction_trend_mart(df):
    try:
        transaction_trend_mart = df.groupBy("transaction_date", "transaction_month", "transaction_year").agg(
            count("transaction_id").alias("daily_transactions"),
            sum(when(col("transaction_status") == "Success", col("amount")).otherwise(0.0)).alias("daily_gmv"),
            (sum(when(col("transaction_status") == "Success", 1).otherwise(0)) / count("transaction_id") * 100).alias("daily_success_rate"),
            avg("amount").alias("avg_transaction_value"),
            count_distinct("customer_id").alias("unique_customers"),
            count_distinct("merchant_id").alias("unique_merchants"),
            # Tracking volume specifically during the Evening peak segment we defined
            sum(when(col("is_peak_hour") == True, 1).otherwise(0)).alias("peak_hour_volume")
        ).sort(col("transaction_date").desc())

        return transaction_trend_mart
    
    except Exception as e:
        print(f"Creating Transaction Trend Mart Failed, due to {e}")


# COMMAND ----------

# Function Purpose: High-level snapshot of total ecosystem health for leadership dashboards.
# Grain: Global (Single Row summary).
# Key Metrics: Total GMV, Network Success Rate, and Active User counts.
def building_executive_kpi_mart(df):
    try:
        executive_kpi_mart = df.agg(
            count("transaction_id").alias("total_transactions"),
            sum(when(col("transaction_status") == "Success", col("amount")).otherwise(0.0)).alias("total_gmv"),
            (sum(when(col("transaction_status") == "Success", 1).otherwise(0)) / count("transaction_id") * 100).alias("overall_success_rate"),
            avg("amount").alias("avg_transaction_size"),
            count_distinct("customer_id").alias("active_customers"),
            count_distinct("merchant_id").alias("active_merchants"),
            mode("payment_method").alias("top_payment_method"),
            mode("transaction_hour").alias("peak_transaction_hour")
        )

        return executive_kpi_mart
    
    except Exception as e:
        print(f"Creating Executive KPI Mart Failed, due to {e}")



# COMMAND ----------

# Saving All Marts to gold layer
def save_marts_to_gold(marts_dict):
   base_path = "/Volumes/workspace/default/payflowdatastorage/gold/"
    
   for mart_name, df in marts_dict.items():
    try:
        target_path = f"{base_path}{mart_name}.delta"            
        df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(target_path)
                        
    except Exception as e:
        print(f"Failed to save {mart_name} due to: {e}")

# COMMAND ----------

def orchestration_pipeline(): 
    try:
        logger.info("Gold Layer to Marts Orchestration Pipeline Started....")
        df = load_data("/Volumes/workspace/default/payflowdatastorage/gold/gold_enriched_transactions.delta")
        logger.info(f"Gold Table Loaded with Shape: {df.count(), len(df.columns)}")

        merchants_mart = building_merchants_mart(df)
        logger.info(f"Merchants Mart Created with Shape: {merchants_mart.count(), len(merchants_mart.columns)}")

        customers_mart = building_customers_mart(df)
        logger.info(f"Customers Mart Created with Shape: {customers_mart.count(), len(customers_mart.columns)}")

        payment_behavior_mart = building_payment_behavior_mart(df)
        logger.info(f"Payment Behavior Mart Created with Shape: {payment_behavior_mart.count(), len(payment_behavior_mart.columns)}")

        transaction_trend_mart = building_transaction_trend_mart(df)
        logger.info(f"Transaction Trend Mart Created with Shape: {transaction_trend_mart.count(), len(transaction_trend_mart.columns)}")

        executive_kpi_mart = building_executive_kpi_mart(df)
        logger.info(f"Executive Kpi Mart Created with Shape: {executive_kpi_mart.count(), len(executive_kpi_mart.columns)}")

        all_marts = {
            "merchants_mart": merchants_mart,
            "customers_mart": customers_mart,
            "payment_behavior_mart": payment_behavior_mart,
            "transaction_trend_mart": transaction_trend_mart,
            "executive_kpi_mart": executive_kpi_mart
        }
        save_marts_to_gold(all_marts)
        logger.info("All Marts Successfully Saved in Gold Layer")
        logger.info("Gold Layer to Marts Pipeline Successfully Completed.")

    except Exception as e:
        logger.error(f"Gold Layer to Marts Orchestration Pipeline Falied, due to {e}")

# COMMAND ----------

orchestration_pipeline()