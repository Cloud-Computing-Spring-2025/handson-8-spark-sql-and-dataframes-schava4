from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Extract individual hashtags
hashtags_df = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))
hashtags_df = hashtags_df.withColumn("Hashtag", lower(trim(col("Hashtag"))))

# Count frequency of each hashtag
hashtag_counts = hashtags_df.groupBy("Hashtag").count().orderBy(col("count").desc()).limit(10)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)