# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("apple_analysis_project").getOrCreate()

# COMMAND ----------

# MAGIC %run "/Users/nadarpravin001@gmail.com/apple_analysis_data_engineering_project/extractor"

# COMMAND ----------

# MAGIC %run "/Users/nadarpravin001@gmail.com/apple_analysis_data_engineering_project/transform"

# COMMAND ----------

# MAGIC %run "/Users/nadarpravin001@gmail.com/apple_analysis_data_engineering_project/loader"

# COMMAND ----------

class FirstWorkFLow:
    """
    ETL pipeline to generate the data for all customers who have bought airpods just after Iphone
    """
    def __init__(self):
        pass

    def runner(self):

        # STEP 1:Extract all required from different source
        input_df = AirpodsAfterIphoneExtractor().extract()

        # STEP 2: Implement the transformation logic
        # Customer who have bought Airpods after Iphone
        AirpodsAfterIphoneDf = AirPodsAfterIphoneTransformer().transform(input_df)

        # STEP 3: Load all required data to different Sink
        AirPodsAfterIphoneLoader(AirpodsAfterIphoneDf).sink()



# firstWorkFlow = FirstWorkFLow().runner()

# COMMAND ----------

class SecondWorkFLow:
    """
    ETL pipeline to generate the data for all customers who have bought only Iphone and AIrpods customers
    """
    def __init__(self):
        pass

    def runner(self):

        # STEP 1:Extract all required from different source
        input_df = AirpodsAfterIphoneExtractor().extract()

        # STEP 2: Implement the transformation logic
        # Customer who have bought Airpods after Iphone
        OnlyAirpodsAndIphonedDf = AirpodsAndIphoneTransformer().transform(input_df)

        # STEP 3: Load all required data to different Sink
        OnlyAirPodsIphoneLoader(OnlyAirpodsAndIphonedDf).sink()



# secondWorkFlow = SecondWorkFLow().runner()

# COMMAND ----------

class ThirdWorkFLow:
    """
    ETL pipeline to list all the products bought by customers after their initial purchase
    """
    def __init__(self):
        pass

    def runner(self):

        # STEP 1:Extract all required from different source
        input_df = AirpodsAfterIphoneExtractor().extract()

        # STEP 2: Implement the transformation logic
        # list all the products bought by customers after their initial purchase
        allProductsAfterInitialDf = AllProductsAfterInitial().transform(input_df)

        # STEP 3: Load all required data to different Sink
        AllProductsAfterInitialLoader(allProductsAfterInitialDf).sink()



# thirdWorkFlow = ThirdWorkFLow().runner()

# COMMAND ----------

class FourthWorkFLow:
    """
    Top 3 products by category by total revenue
    """
    def __init__(self):
        pass

    def runner(self):

        # STEP 1:Extract all required from different source
        input_df = RevenueByCategory().extract()

        # STEP 2: Implement the transformation logic
        # list all the products bought by customers after their initial purchase
        CategoryRevenueDf = CategoryRevenueTransformer().transform(input_df)

        # STEP 3: Load all required data to different Sink
        CategoryRevenueLoader(CategoryRevenueDf).sink()



fourthWorkFlow = FourthWorkFLow().runner()

# COMMAND ----------

class WorkFlowRunner:
    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "firstWorkFlow":
            return FirstWorkFLow().runner()
        elif self.name == "secondWorkFlow":
            return SecondWorkFLow().runner()
        elif self.name == "thirdWorkFlow":
            return ThirdWorkFLow().runner()
        elif self.name == "fourthWorkFlow":
            return FourthWorkFLow().runner()
        else:
            raise ValueError(f"Not implemented for Data Type: {self.name}")

name = "fourthWorkFlow"

first_workFlow_output = WorkFlowRunner(name).runner()


# COMMAND ----------

