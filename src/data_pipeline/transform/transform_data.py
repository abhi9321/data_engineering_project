from pyspark.sql.functions import col, upper


def transform_data(df):
    # transform_df = df.withColumn("country_uppercase", col["Country"].cast("string").upper())
    transform_df = df.withColumn("country_uppercase", upper(col("country")))
    return transform_df
