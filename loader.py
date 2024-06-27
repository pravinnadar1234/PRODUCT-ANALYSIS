# Databricks notebook source
# MAGIC %run "/Users/nadarpravin001@gmail.com/apple_analysis_data_engineering_project/loader_factory"

# COMMAND ----------

class AbstractLoader:

    def __init__(self, tranformedDf):
        self.tranformedDf = tranformedDf

    def sink(self):
        pass

class AirPodsAfterIphoneLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
            sink_type = "dbfs"
            ,df = self.tranformedDf
            ,path = "dbfs:/FileStore/tables/apple_data/output/airpodsAfterApple"
            ,method = "overwrite"
        ).load_data_frame()

class OnlyAirPodsIphoneLoader(AbstractLoader):

    def sink(self):

        parameter = {
            "partitionByColumns": ["location"]
        }
        get_sink_source(
            sink_type = "dbfs_with_partition",
            df = self.tranformedDf, 
            path = "dbfs:/FileStore/tables/apple_data/output/airpodsOnlyApple", 
            method = "overwrite",
            parameter = parameter
        ).load_data_frame()

        get_sink_source(
            sink_type = "delta",
            df = self.tranformedDf, 
            path = "default.onlyAirpodsAndIphone", 
            method = "overwrite",
        ).load_data_frame()


class AllProductsAfterInitialLoader(AbstractLoader):
    def sink(self):
        get_sink_source(
            sink_type = "dbfs"
            ,df = self.tranformedDf
            ,path = "dbfs:/FileStore/tables/apple_data/output/allProductsAfterInitial"
            ,method = "overwrite"
        ).load_data_frame()

        
class CategoryRevenueLoader(AbstractLoader):
    def sink(self):
        parameter = {
            "partitionByColumns": ["category"]
        }

        get_sink_source(
            sink_type = "dbfs_with_partition"
            ,df = self.tranformedDf
            ,path = "dbfs:/FileStore/tables/apple_data/output/categoryWiseRevenue"
            ,method = "overwrite"
            ,parameter = parameter
        ).load_data_frame()


# COMMAND ----------

