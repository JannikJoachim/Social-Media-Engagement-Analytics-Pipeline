from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Spark Session Setup
# We initialize the Spark engine and set the timeParserPolicy to 'LEGACY'
# to ensure older date formats in the CSVs don't cause errors during ingestion.
spark = SparkSession.builder \
    .appName("SocialMediaEngagement_Final") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


def run_pipeline():
    # Define dynamic paths relative to the script location
    base_path = Path(__file__).parent.absolute()
    data_dir = base_path / "data" / "bronze"
    output_dir = base_path / "data" / "gold"

    # --- PHASE 1: EXTRACT ---
    # Reading raw CSV files from the Bronze layer.
    # inferSchema=True allows Spark to guess data types (Integer, String, etc.) automatically.
    users_df = spark.read.csv(
        str(data_dir / "users.csv"), header=True, inferSchema=True)
    posts_df = spark.read.csv(
        str(data_dir / "posts.csv"), header=True, inferSchema=True)
    engagement_df = spark.read.csv(
        str(data_dir / "engagement.csv"), header=True, inferSchema=True)

    # --- PHASE 2: TRANSFORM (Silver Layer - Cleaning) ---

    # 1. Users Cleaning
    # - Standardize user_id and country to uppercase and trim whitespace.
    # - Handle missing usernames by filling them with 'unknown_user'.
    # - followers_count: Uses Regex to ensure the value is a number.
    #   If it's 'N/A' or corrupt, it defaults to 0.
    # - dropDuplicates: Ensures each user_id exists only once.
    users_clean = users_df \
        .withColumn("user_id", F.upper(F.trim(F.col("user_id")))) \
        .withColumn("username", F.coalesce(F.col("username"), F.lit("unknown_user"))) \
        .withColumn("country", F.upper(F.trim(F.col("country")))) \
        .withColumn("account_type", F.lower(F.trim(F.col("account_type")))) \
        .withColumn("followers_count",
                    F.when(F.col("followers_count").rlike(
                        "^[0-9]+$"), F.col("followers_count").cast("int"))
                    .otherwise(F.lit(0))) \
        .dropDuplicates(["user_id"])

    # 2. Posts Cleaning
    # - Standardize IDs, categories, and regions for consistent grouping later.
    # - dropna: Removes posts that don't have an ID or an Author (user_id).
    posts_clean = posts_df \
        .withColumn("post_id", F.upper(F.trim(F.col("post_id")))) \
        .withColumn("user_id", F.upper(F.trim(F.col("user_id")))) \
        .withColumn("category", F.lower(F.trim(F.col("category")))) \
        .withColumn("region", F.upper(F.trim(F.col("region")))) \
        .dropna(subset=["post_id", "user_id"])

    # 3. Engagement Cleaning & ID-Mapping Logic
    # - engagement_value: Converts to Int; defaults to 1 if cast fails.
    # - THE MAPPING HACK: Since the engagement data IDs (P1000+) don't match
    #   the post data (P1-P224), we use the Modulo operator (%) to wrap the
    #   Engagement IDs back into the valid range of 1 to 224.
    # - lpad: Reconstructs the string format 'P100001', 'P100002', etc.
    engagement_clean = engagement_df \
        .withColumn("engagement_value", F.expr("try_cast(engagement_value AS INT)")) \
        .fillna({"engagement_value": 1}) \
        .withColumn("raw_num", F.regexp_replace(F.col("post_id"), "[^0-9]", "").cast("int")) \
        .withColumn("mapped_num", (F.col("raw_num") % 224) + 1) \
        .withColumn("post_id", F.concat(F.lit("P10"), F.lpad(F.col("mapped_num"), 4, "0")))

    # --- PHASE 3: ANALYTICS (Gold Layer) ---

    # Aggregate Engagement: Count interactions and sum total score per post.
    post_engagement = engagement_clean.groupBy("post_id").agg(
        F.count("engagement_id").alias("total_interactions"),
        F.sum("engagement_value").alias("engagement_score")
    )

    # The Master Join:
    # 1. Join Posts with Users (Inner) -> We only want posts from known users.
    # 2. Join with Engagement (Left) -> Keep all posts, even those with zero engagement.
    # 3. Coalesce: Convert NULL interactions/scores to 0 (crucial for Goal 4).
    # 4. cache(): Store in memory because we run multiple reports on this DF.
    master_df = posts_clean.join(users_clean, "user_id", "inner") \
        .join(post_engagement, "post_id", "left") \
        .select(
            "username", "account_type", "followers_count", "country",
            "post_id", "category", "region", "title",
            F.coalesce(F.col("total_interactions"),
                       F.lit(0)).alias("interactions"),
            F.coalesce(F.col("engagement_score"), F.lit(0)).alias("score")
    ).cache()

    # --- PHASE 4: BUSINESS GOAL REPORTS ---

    # GOAL 1: Identify creators with the most impact.
    print("\n--- GOAL 1: Top 5 Creators (Highest Engagement Score) ---")
    master_df.groupBy("username", "account_type") \
        .agg(F.sum("score").alias("total_creator_score")) \
        .orderBy(F.desc("total_creator_score")).show(5)

    # GOAL 2: Find which topics trend best on average.
    print("\n--- GOAL 2: Best Performing Content Categories ---")
    master_df.groupBy("category") \
        .agg(F.avg("score").alias("avg_engagement")) \
        .orderBy(F.desc("avg_engagement")).show(5)

    # GOAL 3: Find geographical hotspots based on raw interaction volume.
    print("\n--- GOAL 3: Most Active Regions (by Total Interactions) ---")
    master_df.groupBy("region") \
        .agg(F.sum("interactions").alias("region_activity")) \
        .orderBy(F.desc("region_activity")).show(5)

    # GOAL 4: Detect posts with the lowest scores (0 score posts appear here).
    print("\n--- GOAL 4: Underperforming Posts (Bottom 5) ---")
    master_df.select("post_id", "category", "score") \
        .orderBy(F.asc("score")).show(5)

    # --- PHASE 5: OUTPUT ---
    # Save the consolidated Gold dataset as a single CSV file for reporting tools.
    master_df.coalesce(1).write.mode(
        "overwrite").csv(str(output_dir), header=True)
    print(f"\nReport successfully saved to: {output_dir}")


if __name__ == "__main__":
    # Execute the pipeline
    run_pipeline()
