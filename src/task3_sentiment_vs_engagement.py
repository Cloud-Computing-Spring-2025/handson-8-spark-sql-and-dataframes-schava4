from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment and analyze engagement
sentiment_stats = (
    posts_df
    .withColumn("Sentiment",
        when(col("SentimentScore") > 0.3, "Positive")
        .when(col("SentimentScore") < -0.3, "Negative")
        .otherwise("Neutral"))
    .groupBy("Sentiment")
    .agg(
        avg("Likes").alias("AvgLikes"),
        avg("Retweets").alias("AvgRetweets")
    )
    .orderBy("Sentiment"))

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)