# Databricks notebook source
class Transformer:

    def __init__(self):
        pass

    def transform(self):
        pass

class AirPodsAfterIphoneTransformer(Transformer):

    def transform(self, input_df):
        """
        Customer who have bought Airpods after Iphone
        """

        print("transactionInputDf in tranform")
        transactionInputDf = input_df.get("transactionInputDf")

        # transactionInputDf.show()

        window = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = (
            transactionInputDf.withColumn("next_product_name", lead("product_name").over(window))
                              .filter((col("product_name") == "iPhone") & (col("next_product_name") == "AirPods"))
        )

        print("Airpods after buying Apple")
        transformedDF.show()

        customerInputDf = input_df.get("customerInputDf")

        final_df = (
            customerInputDf.join(broadcast(transformedDF),"customer_id")
                           .select(transformedDF["customer_id"], customerInputDf["customer_name"], customerInputDf["location"])
        )

        print("Final data")

        final_df.show()
        return final_df
    

class AirpodsAndIphoneTransformer(Transformer):

    def transform(self, input_df):
        """
        customers who have bought only Iphone and AIrpods customers
        """
        transactionInputDf = input_df.get("transactionInputDf")

        transformedDF = (
            transactionInputDf.groupBy("customer_id")
                              .agg(collect_set("product_name").alias("Product_Name_list"))
                              .filter(
                                  (array_contains(col("Product_Name_list"), "iPhone")) &
                                  (array_contains(col("Product_Name_list"), "AirPods")) &
                                  (size(col("Product_Name_list")) ==  2)
                              )
                              .drop("Product_Name_list")
                              .select("customer_id")
        )

        print("Transformed Data frame")
        transformedDF.show()

        customerInputDf = input_df.get("customerInputDf")

        final_df = (
            customerInputDf.join(broadcast(transformedDF),"customer_id")
                           .select(transformedDF["customer_id"], customerInputDf["customer_name"], customerInputDf["location"])
        )

        print("Final data")

        final_df.show()
        return final_df


class AllProductsAfterInitial(Transformer):

    def transform(self, input_df):
        """
        all the products bought by customers after their initial purchase
        """

        print("transactionInputDf in tranform")
        transactionInputDf = input_df.get("transactionInputDf")

        # transactionInputDf.show()

        window = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = (
           transactionInputDf.withColumn("rank", row_number().over(window))
                             .filter(col("rank") > 1)
                             .drop("rank")
        )

        print("Ranked transformed Data frame")
        transformedDF.show()

        print("grouped Data Frame")

        groupedDf = (
            transformedDF.groupBy("customer_id")
                         .agg(collect_list("product_name").alias("Products"))
        )

        groupedDf.show()

        customerInputDf = input_df.get("customerInputDf")

        print("Final df")

        final_df = (
            customerInputDf.join(groupedDf,"customer_id")
                           .select(transformedDF["customer_id"], customerInputDf["customer_name"], customerInputDf["location"]
                                   ,groupedDf["Products"])
        )

        final_df.show()
        return final_df


class CategoryRevenueTransformer(Transformer):

    def transform(self, input_df):
        """
        Top 3 products by category by total revenue
        """
        transactionInputDf = input_df.get("transactionInputDf")

        productInputDf = input_df.get("productInputDf")

        productInputDf = (
            productInputDf.withColumn("product_name",split(col("product_name"), " ").getItem(0))
        )

        productInputDf.show()

        print("Joined data frame")

        joined_df = (
            transactionInputDf.join(broadcast(productInputDf),"product_name")
        )

        joined_df.show()

        print(" Transformed Data Frame")

        transformedDf = (
            joined_df.groupBy("category")
                     .agg(sum("price").alias("Revenue"))
        )

        transformedDf.show()

        return transformedDf




# COMMAND ----------

