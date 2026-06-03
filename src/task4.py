from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task4 - Top Verified Users") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load data
posts = spark.read.csv("input/posts.csv", header=True, inferSchema=True)
users = spark.read.csv("input/users.csv", header=True, inferSchema=True)

# Register as SQL temp views
posts.createOrReplaceTempView("posts")
users.createOrReplaceTempView("users")

# SQL query: top verified users ranked by total engagement
result = spark.sql("""
    SELECT u.UserID,
           u.Username,
           u.Country,
           COUNT(p.PostID)                AS TotalPosts,
           SUM(p.Likes)                   AS TotalLikes,
           SUM(p.Retweets)                AS TotalRetweets,
           SUM(p.Likes + p.Retweets)      AS TotalEngagement
    FROM posts p
    JOIN users u ON p.UserID = u.UserID
    WHERE u.Verified = true
    GROUP BY u.UserID, u.Username, u.Country
    ORDER BY TotalEngagement DESC
    LIMIT 10
""")

result.show()

# Save output
result.coalesce(1).write.csv("outputs/top_verified_users.csv", header=True, mode="overwrite")

print("✅ Task 4 complete: top_verified_users.csv saved to outputs/")

spark.stop()
