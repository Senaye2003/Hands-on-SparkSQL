from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim, count, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task1 - Hashtag Trends") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load posts data
posts = spark.read.csv("input/posts.csv", header=True, inferSchema=True)

# Explode hashtags (each post has comma-separated hashtags)
hashtag_df = posts.select(
    col("PostID"),
    explode(split(col("Hashtags"), ",")).alias("Hashtag")
)

# Clean whitespace
hashtag_df = hashtag_df.withColumn("Hashtag", trim(col("Hashtag")))

# Register as SQL temp view
hashtag_df.createOrReplaceTempView("hashtags")

# SQL query: count and rank hashtags
result = spark.sql("""
    SELECT Hashtag,
           COUNT(*) AS UsageCount
    FROM hashtags
    GROUP BY Hashtag
    ORDER BY UsageCount DESC
""")

result.show()

# Save output
result.coalesce(1).write.csv("outputs/hashtag_trends.csv", header=True, mode="overwrite")

print("✅ Task 1 complete: hashtag_trends.csv saved to outputs/")

spark.stop()
