# Databricks notebook source
# PARENT CLASS
class DataSource:
    """
    Abstract class
    """

    # CONSTRCUTOR METHOD
    def __init__(self, path):
        self.path = path

    # METHOD
    def get_data_frame(self):
        """
        Abstarct method. Function will be defined in sub classes
        """

        raise ValueError("Not Implemeted")


# CHILD class
class CSV_data_source(DataSource):

    # METHOD OVERRIDING
    def get_data_frame(self):

        return (
            spark.read.format("csv")
                      .option("header","true")
                      .load(self.path)
        )

class Parquet_data_source(DataSource):

    # METHOD OVERRIDING
    def get_data_frame(self):

        return (
            spark.read.format("parquet").load(self.path)
        )

class Delta_data_source(DataSource):

    # METHOD OVERRIDING
    def get_data_frame(self):

        table_name = self.path
        return (
            spark.read.table(table_name)
        )

# Function to decid ewhich subclass to call based on data_type
def get_data_source(data_type, file_path):

    if data_type == "csv":
        return CSV_data_source(file_path)
    elif data_type == "parquet":
        return Parquet_data_source(file_path)
    elif data_type == "delta":
        return Delta_data_source(file_path)
    else:
        raise ValueError(f"Not implemented for Data Type: {data_type}")

# COMMAND ----------

