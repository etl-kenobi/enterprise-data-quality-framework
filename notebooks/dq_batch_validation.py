from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Define DQValidator class
class DQValidator:
    def __init__(self, df):
        self.df = df

    def check_nulls(self, columns):
        return self.df.filter(
            " OR ".join([f"{col} IS NULL" for col in columns])
        )

    def check_duplicates(self, subset):
        return self.df.groupBy(subset).count().filter("count > 1")

    def check_pattern(self, column, pattern):
        return self.df.filter(~col(column).rlike(pattern))

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:sqlserver://azure-sql-rahu.database.windows.net:1433;database=azure-sql"
jdbc_properties = {
    "user": "*****",
    "password": "******",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Step 1: Read staging data
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "CustomerStaging") \
    .option("user", jdbc_properties["user"]) \
    .option("password", jdbc_properties["password"]) \
    .load()

df.show()

# Step 2: Run DQ checks using validator
validator = DQValidator(df)

nulls_df = validator.check_nulls(["customer_id", "name", "email"])
dups_df = validator.check_duplicates(["customer_id"])
pattern_df = validator.check_pattern("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")

# Step 3: Add flags for reason tagging
df_flagged = df.withColumn("is_customer_id_null", col("customer_id").isNull()) \
    .withColumn("is_name_null", col("name").isNull()) \
    .withColumn("is_email_null", col("email").isNull()) \
    .withColumn("is_email_valid", ~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"))

# Step 4: Validated and rejected datasets
validated_df = df_flagged.filter(
    (~col("is_customer_id_null")) &
    (~col("is_name_null")) &
    (~col("is_email_null")) &
    (~col("is_email_valid"))
).select("customer_id", "name", "email", "country")

rejected_df = df_flagged.filter(
    col("is_customer_id_null") |
    col("is_name_null") |
    col("is_email_null") |
    (col("is_email_valid"))
).withColumn(
    "reason",
    when(col("is_customer_id_null"), lit("Null customer_id"))
    .when(col("is_name_null"), lit("Null name"))
    .when(col("is_email_null"), lit("Null email"))
    .when(col("is_email_valid"), lit("Invalid email format"))
).select("customer_id", "name", "email", "country", "reason")

# Step 5: Write to CustomerValidated
validated_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "CustomerValidated") \
    .option("user", jdbc_properties["user"]) \
    .option("password", jdbc_properties["password"]) \
    .mode("overwrite") \
    .save()

# Step 6: Write to CustomerRejected
rejected_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "CustomerRejected") \
    .option("user", jdbc_properties["user"]) \
    .option("password", jdbc_properties["password"]) \
    .mode("overwrite") \
    .save()

print("✅ Data validation complete. Records loaded to CustomerValidated and CustomerRejected.")
