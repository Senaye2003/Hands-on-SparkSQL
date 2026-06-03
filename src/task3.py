from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task3 - Sentiment vs Engagement") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load posts data
posts = spark.read.csv("input/posts.csv", header=True, inferSchema=True)

# Register as SQL temp view
posts.createOrReplaceTempView("posts")

# SQL query: bucket sentiment scores into Negative / Neutral / Positive
result = spark.sql("""
    SELECT SentimentBucket,
           COUNT(*)                    AS TotalPosts,
           ROUND(AVG(Likes), 2)        AS AvgLikes,
           ROUND(AVG(Retweets), 2)     AS AvgRetweets,
           ROUND(AVG(Likes + Retweets), 2) AS AvgTotalEngagement
    FROM (
        SELECT *,
               CASE
                   WHEN SentimentScore < -0.33 THEN 'Negative'
                   WHEN SentimentScore <= 0.33 THEN 'Neutral'
                   ELSE 'Positive'
               END AS SentimentBucket
        FROM posts
    ) bucketed
    GROUP BY SentimentBucket
    ORDER BY AvgTotalEngagement DESC
""")

result.show()

# Save output
result.coalesce(1).write.csv("outputs/sentiment_engagement.csv", header=True, mode="overwrite")

print("✅ Task 3 complete: sentiment_engagement.csv saved to outputs/")

spark.stop()
