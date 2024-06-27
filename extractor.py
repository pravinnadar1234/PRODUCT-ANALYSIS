# Databricks notebook source
# MAGIC %run "/Users/nadarpravin001@gmail.com/apple_analysis_data_engineering_project/reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class
    """
    def __init__(self):
        pass

    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):

    """
    Implemting steps to extract or read the data 
    """
    def extract(self):
        # reading transaction file (CSV)
        transactionInputDf = get_data_source(
            data_type = "csv"
            ,file_path = "dbfs:/FileStore/tables/apple_data/Transaction_Updated.csv"
        
        ).get_data_frame()

        # reading customer data  file (DELTA)
        customerInputDf = get_data_source(
            data_type = "delta"
            ,file_path = "default.customer_apple_delta"
        
        ).get_data_frame()

        transactionInputDf.orderBy("customer_id","transaction_date").show()

        input_df = {
            "transactionInputDf" : transactionInputDf
            ,"customerInputDf" : customerInputDf
        }

        return input_df
    

class RevenueByCategory(Extractor):

    """
    Implemting steps to extract or read the data 
    """
    def extract(self):
        # reading transaction file (CSV)
        transactionInputDf = get_data_source(
            data_type = "csv"
            ,file_path = "dbfs:/FileStore/tables/apple_data/Transaction_Updated.csv"
        
        ).get_data_frame()

        # reading product data  file (CSV)
        productInputDf = get_data_source(
            data_type = "csv"
            ,file_path = "dbfs:/FileStore/tables/apple_data/Products_Updated.csv"
        
        ).get_data_frame()

        transactionInputDf.orderBy("customer_id","transaction_date").show()

        input_df = {
            "transactionInputDf" : transactionInputDf
            ,"productInputDf" : productInputDf
        }

        return input_df


    

# COMMAND ----------

