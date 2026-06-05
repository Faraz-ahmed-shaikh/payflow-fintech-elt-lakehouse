# Databricks notebook source
# MAGIC %md
# MAGIC #### Importing & Defining Loading Function

# COMMAND ----------

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

# This Function will load our transactions and merchants data from bronze layer
def load_data(path): 
    try: 
        df = spark.read.format("delta").load(path)
        return df
    
    except Exception as e:
        print(f"There was a problem in loading data: {e}") 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning Functions for Transactions Data

# COMMAND ----------

# Renaming Column Function
def renaming_columns(bronze_transactions): 
    try: 
        # Renaming columns 
        bronze_transactions = bronze_transactions.withColumnsRenamed({
            "city": "customer_city",
            "currency": "transaction_currency"
        })
        return bronze_transactions
    
    except Exception as e:
        print(f"Column Renaming Failed due to {e}")

# Category Data Standardization 
def standardize_categorical_data(transactions_df): 
    try: 
        # Standardizing Transaction Currency, first we will convert values to UPPER case, then we will mapp with a fallback.
        transactions_df = transactions_df.withColumn("transaction_currency", upper(trim(transactions_df.transaction_currency)))
        transactions_df = transactions_df.withColumn("transaction_currency", 
        when(col("transaction_currency").isin("₹", "INR ₹", "₹ INR", "INR"),"INR")
        .when(col("transaction_currency").isin("$", "$ USD", "USD $", "USD"),"USD")
        .when(col("transaction_currency").isin("€", "€ EUR", "EUR €", "EUR"),"EUR")
        .when(col("transaction_currency").isin("AED"),"AED")
        .when(col("transaction_currency").isin("£", "£ GBP", "GBP £", "GBP"),"GBP")
        .when(col("transaction_currency").isin("¥", "¥ YEN", "YEN ¥", "YEN", "JPY"),"YEN")
        .otherwise(col("transaction_currency")))

        # Standardizing Customers City, by Title Case
        transactions_df = transactions_df.withColumn("customer_city", initcap(trim(transactions_df.customer_city)))

        # Standardizing Device Type, by Title case and mapping
        transactions_df = transactions_df.withColumn("device_type", initcap(trim(transactions_df.device_type)))
        transactions_df = transactions_df.withColumn("device_type", 
            when(col("device_type").isin("Android", "Android Platform"),"Android")
            .when(col("device_type").isin("Ios", "Iphone", "I Phone","Apple", "Ios App"),"IOS")
            .when(col("device_type").isin("Web", "Website", "Web Portal", "Browser"),"Website")
            .when(col("device_type").isin("Ussd", "Unstructured Supplementary Service Data"),"USSD")
            .otherwise("Unknown Device"))
        
        # Standardizing Payment Method, by Title case and mapping
        transactions_df = transactions_df.withColumn("payment_method", initcap(trim(regexp_replace(col("payment_method"), "[_-]", " "))))
        transactions_df = transactions_df.withColumn("payment_method", 
            when(col("payment_method").isin("Credit Card", "Cc", "Cc Card", "Credit"), "Credit Card")
            .when(col("payment_method").isin("Wallet", "E Wallet", "Onilne Wallet", "App Wallet"), "E-Wallet")
            .when(col("payment_method").isin("Debit Card", "Db", "Db Card", "Debit"), "Debit Card")
            .when(col("payment_method").isin("Upi", "Upi Payment"), "UPI")
            .when(col("payment_method").isin("Netbanking", "Net Banking"), "Net Banking")
            .otherwise("Other Method")
            )
        
        # Standardizing Transactions Status by using Title Case
        transactions_df = transactions_df.withColumn("transaction_status", initcap(trim(transactions_df.transaction_status)))

        return transactions_df

    except Exception as e:
        print(f"Standardization of Text failed due to {e}")

# Standardizing Transactions Timestamp with Mixed formats         
def transaction_timestamp_parsing(transactions_df): 
    try: 
        transactions_df = transactions_df.withColumn(
            "transaction_timestamp",
            when(
                col("transaction_timestamp").rlike(
                    r"^\d{4}-\d{2}-\d{2}"
                ),
                try_to_timestamp(
                    col("transaction_timestamp"),
                    lit("yyyy-MM-dd HH:mm:ss")
                )
            ).when(
                col("transaction_timestamp").rlike(
                    r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$"
                ),
                try_to_timestamp(
                    col("transaction_timestamp"),
                    lit("dd-MM-yyyy HH:mm:ss")
                )
            ).when(
                col("transaction_timestamp").rlike(
                    r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$"
                ),
                try_to_timestamp(
                    col("transaction_timestamp"),
                    lit("dd-MM-yyyy HH:mm")
                )
            )
        )

        return transactions_df

    except Exception as e:
        print(f"Timestamp parsing failed due to {e}")

# Function for Filling Nulls and Changing Datatypes
def fixing_null_and_datatypes(transactions_df): 
    try:
        # Filling Nulls (For some columns nulls will be filled by mapping in Standardization)
        transactions_df = transactions_df.fillna({
            "customer_city": "Unknown City",
            "merchant_id": "Unknown Merchant",
            "payment_method": "Unknown Method",
            "transaction_status": "Unknown Status",
            "device_type": "Unknown Device"
        })
        # Changing the DataTypes
        transactions_df = transactions_df.withColumns({
            "transaction_id": col("transaction_id").cast("string"),
            "customer_id": col("customer_id").cast("string"),
            "merchant_id": col("merchant_id").cast("string"),
            "amount": col("amount").cast("double"),
            "transaction_currency": col("transaction_currency").cast("string"),
            "transaction_timestamp": col("transaction_timestamp").cast("timestamp"),
            "payment_method": col("payment_method").cast("string"),
            "transaction_status": col("transaction_status").cast("string"),
            "customer_city": col("customer_city").cast("string"), 
            "device_type": col("device_type").cast("string"),
            "data_source": col("data_source").cast("string"),
            "layer": col("layer").cast("string"),
            "ingested_at": col("ingested_at").cast("timestamp")
        })

        return transactions_df

    except Exception as e:
        print(f"There was a problem in fixing nulls and datatypes {e}")

# This is a combined function for sanity-check, dropping duplicates, adding metadata and reordering columns 
def sanity_checks_and_validations(transactions_df): 
    try: 
        rows_before = transactions_df.count()
        # Amount Sanity-check, keeping rows where amount is above 0 and below & equal to 1,00,000 as it is the limit
        transactions_df = transactions_df.filter((col("amount")>0)&(col("amount")<=100000))
        # Dropping Duplicate Transactions
        transactions_df = transactions_df.dropDuplicates(["transaction_id"])
        rows_after = transactions_df.count()
        print(f"{rows_before-rows_after} Rows Removed while sanity-check of amount and duplicates")
        # Added Updated Metadata
        transactions_df = transactions_df.withColumns({
            "layer": lit("silver"),
            "transformed_at": current_timestamp()
        })
        # Recording Columns
        transactions_df = transactions_df.select('transaction_id', 'customer_id', 'merchant_id', 'amount', 'transaction_currency', 'transaction_timestamp', 'payment_method', 'transaction_status', 'customer_city', 'device_type', 'data_source', 'layer', 'ingested_at', "transformed_at")

        return transactions_df

    except Exception as e:
        print(f"There was a problem in sanity checks and validations {e}")

# COMMAND ----------

def transactions_data_cleaning(bronze_transactions):
    try: 
        transactions_df = renaming_columns(bronze_transactions)
        transactions_df = standardize_categorical_data(transactions_df)
        transactions_df = transaction_timestamp_parsing(transactions_df)
        transactions_df = fixing_null_and_datatypes(transactions_df)
        transactions_df = sanity_checks_and_validations(transactions_df)
        return transactions_df
    
    except Exception as e: 
        print(f"There was a problem in Cleaning Transactions data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning Functions For Merchant Data Cleaning 

# COMMAND ----------

# This function will rename merchants columns 
def renaming_merchants_cols(bronze_merchants): 
    try: 
        merchants_df = bronze_merchants.withColumnRenamed("city", "merchant_city")
        return merchants_df
    
    except Exception as e:
        print(f"Renaming Columns Failed, due to {e}")

# Dropping duplicates based on merchant_id 
def dedup_merchants(merchants_df):
    try: 
        before_drop = merchants_df.count()
        merchants_df = merchants_df.dropDuplicates(["merchant_id"])
        print(f"{before_drop - merchants_df.count()} Duplicate Rows Removed")

        return merchants_df
    
    except Exception as e:
        print(f"Dropping Duplicates Failed, due to {e}")

# Standardizing Categorical Columns
def standardize_cat_cols(merchants_df): 
    try:
        merchants_df = merchants_df.withColumns({
        "merchant_city": initcap(trim(col("merchant_city"))),
        "merchant_category": initcap(trim(col("merchant_category"))),
        "merchant_name": trim(col("merchant_name")),
        "merchant_tier": initcap(trim(col("merchant_tier"))),
        "data_source": lower(trim(col("data_source"))),
        "layer": lower(trim(col("layer")))})
        
        return merchants_df
    
    except Exception as e:
        print(f"Standardization Failed, due to {e}")

# Filling nulls and dropping rows with null id
def merchants_null_handling(merchants_df):
    try: 
        # Handling Nulls
        merchants_df = merchants_df.fillna({
            "merchant_city": "Unknown City",
            "merchant_category": "Unknown Category",
            "merchant_name": "Unknown Merchant",
            "merchant_tier": "Unknown Tier"
        })
        # Sanity-check: Keeping Records who have ID
        before_drop = merchants_df.count()
        merchants_df = merchants_df.filter(col("merchant_id").isNotNull())
        print(f"{before_drop - merchants_df.count()} Rows with Null IDs Removed")

        return merchants_df
    
    except Exception as e:
        print(f"Null Handing Failed, due to {e}")

# Adding Metadata columns and reordering 
def updating_and_reordering_columns(merchants_df):
    try: 
        # Updating and Adding Metadata 
        merchants_df = merchants_df.withColumns({
            "layer": lit("silver"),
            "transformed_at": current_timestamp()
        })
        # Columns Reordering
        merchants_df = merchants_df.select('merchant_id', 'merchant_name', 'merchant_category', 'merchant_city', 'merchant_tier', 'data_source', 'layer', 'ingested_at', 'transformed_at')

        return merchants_df
    
    except Exception as e:
        print(f"Column Updating & Ordering Failed, due to {e}")

# COMMAND ----------

def merchant_data_cleaning(bronze_merchants): 
    try: 
        merchants_df = renaming_merchants_cols(bronze_merchants)
        merchants_df = dedup_merchants(merchants_df)
        merchants_df = standardize_cat_cols(merchants_df)
        merchants_df = merchants_null_handling(merchants_df)
        merchants_df = updating_and_reordering_columns(merchants_df)

        return merchants_df
    
    except Exception as e:
        print("Cleaning Merchants Data Failed, due to {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Storing and Orchestration 

# COMMAND ----------

def store_data(df,path): 
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(path) 

# COMMAND ----------

# Execute the end-to-end pipeline workflow
def orchestration_pipeline():
    try:
        logger.info("Bronze Layer to Silver Layer Orchestration Pipeline Started.....")

        logger.info("\nPhase 1: Cleaning Transactions Data...")
        bronze_transactions = load_data("/Volumes/workspace/default/payflowdatastorage/bronze/transactions.delta")
        logger.info(f"Loaded Transactions Data From Bronze with {bronze_transactions.count()} Rows")
        logger.info("Cleaning Transactions Table Started......")
        transactions_df = transactions_data_cleaning(bronze_transactions)
        logger.info(f"Transaction Table Cleaned.")
        logger.info(f"{transactions_df.count()} Rows Sucessfully Loaded to Silver Layer")
        store_data(transactions_df,"/Volumes/workspace/default/payflowdatastorage/silver/transactions.delta")

        logger.info("\nPhase 2: Cleaning Merchants Data...")
        bronze_merchants = load_data("/Volumes/workspace/default/payflowdatastorage/bronze/merchants.delta")
        logger.info(f"Loaded Merchants Data From Bronze with {bronze_merchants.count()} Rows")
        logger.info("Cleaning Merchants Table Started......")
        merchants_df = merchant_data_cleaning(bronze_merchants)
        logger.info(f"Merchants Table Cleaned.")
        store_data(merchants_df,"/Volumes/workspace/default/payflowdatastorage/silver/merchants.delta")
        logger.info(f"{merchants_df.count()} Rows Sucessfully Loaded to Silver Layer")

        logger.info("Bronze Layer to Silver Layer Pipeline Successfully Completed")

    
    except Exception as e:
        logger.error(f"Bronze Layer to Silver Layer Orchestration Pipeline Failed due to: {e}")

# COMMAND ----------

orchestration_pipeline()