from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Task2 - Engagement by Age Group") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load data
posts = spark.read.csv("input/posts.csv", header=True, inferSchema=True)
users = spark.read.csv("input/users.csv", header=True, inferSchema=True)

# Register as SQL temp views
posts.createOrReplaceTempView("posts")
users.createOrReplaceTempView("users")

# SQL query: join posts + users, aggregate engagement by age group
result = spark.sql("""
    SELECT u.AgeGroup,
           COUNT(p.PostID)        AS TotalPosts,
           SUM(p.Likes)           AS TotalLikes,
           SUM(p.Retweets)        AS TotalRetweets,
           ROUND(AVG(p.Likes), 2) AS AvgLikes,
           ROUND(AVG(p.Retweets), 2) AS AvgRetweets
    FROM posts p
    JOIN users u ON p.UserID = u.UserID
    GROUP BY u.AgeGroup
    ORDER BY TotalLikes DESC
""")

result.show()

# Save output
result.coalesce(1).write.csv("outputs/engagement_by_age.csv", header=True, mode="overwrite")

print("✅ Task 2 complete: engagement_by_age.csv saved to outputs/")

spark.stop()
