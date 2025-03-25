from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Calculate top verified users
top_verified = (
    users_df.filter(col("Verified") == True)
    .join(posts_df, "UserID")
    .withColumn("Reach", col("Likes") + col("Retweets"))
    .groupBy("Username")
    .agg(_sum("Reach").alias("TotalReach"))
    .orderBy(desc("TotalReach"))
    .limit(5)
)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)