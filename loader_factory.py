# Databricks notebook source
# PARENT CLASS
class DataSink:
    """
    Abstract class
    """

    # CONSTRCUTOR METHOD
    def __init__(self,df, path, method, parameter):
        self.df = df
        self.path = path
        self.method = method
        self.parameter = parameter

    # METHOD
    def load_data_frame(self):
        """
        Abstarct method. Function will be defined in sub classes
        """

        raise ValueError("Not Implemeted")

class LoadToDBFS(DataSink):

    # METHOD
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)

class LoadToDBFSWithPartition(DataSink):

    # METHOD
    def load_data_frame(self):

        partitionByColumns = self.parameter.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)


class LoadToDeltaTable(DataSink):

    # METHOD
    def load_data_frame(self):

        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)


def get_sink_source(sink_type, df,  path, method, parameter= None):
    if sink_type == "dbfs":
        return LoadToDBFS(df, path, method, parameter)
    elif sink_type == "dbfs_with_partition":
        return LoadToDBFSWithPartition(df, path, method, parameter)
    elif sink_type == "delta":
        return LoadToDeltaTable(df, path, method, parameter)
    else:
        raise ValueError(f"Not implemented for Data Type: {sink_type}")



# COMMAND ----------

