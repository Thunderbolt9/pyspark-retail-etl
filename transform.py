from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logger import Log4J

if __name__ == "__main__":
    # Initialize Spark Session With 3 Executors
    spark = SparkSession.builder \
            .appName("transform_retail_etl") \
            .master("local[3]") \
            .getOrCreate()
    
    # Initialize Logger
    logger = Log4J(spark)

    # Custom schema for retail data
    retailSchema = """InvoiceNo INT, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING,
                      UnitPrice FLOAT, CustomerID INT, Country STRING"""

    # Create DF for the data
    retailDF = spark.read \
               .format("csv") \
               .option("header", "true") \
               .schema(retailSchema) \
               .load("data/data.csv")
    
    # Schema Exploration
    retailDF.printSchema()

    # Convert Invoice Date To Standard ISO Format
    retailDF = retailDF.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
    
    logger.info("Calculate SalePrice")
    retailDF = retailDF.withColumn("SalePrice", col("Quantity")*col("UnitPrice"))
    retailDF.show(5)


    logger.info("Calculate Daily Revenue")
    dailyRevenueDF = retailDF \
                    .groupBy(to_date("InvoiceDate").alias("InvoiceDate")) \
                    .agg(round(sum("SalePrice"), 2).alias("DailyRevenue"))
    
    dailyRevenueDF.show(5)

    logger.info("Load The Daily Revenue")
    dailyRevenueDF.write \
                       .mode("overwrite") \
                       .parquet("output/daily-revenue")
    

    logger.info("Top selling products")
    TopSellingProductDF = retailDF \
                          .groupBy("StockCode") \
                          .agg(round(sum("SalePrice"), 2).alias("TotalPrice")) \
                          .orderBy(desc("TotalPrice")) \
                          .select("StockCode") \
                          .limit(1)
    
    TopSellingProductDF.show()
    
    logger.info("Load The Top Selling Product")
    TopSellingProductDF.write \
                       .mode("overwrite") \
                       .parquet("output/top-selling-products")

