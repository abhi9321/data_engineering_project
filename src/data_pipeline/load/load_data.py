def load_data(transformed_df, output_path):
    transformed_df.write.csv(output_path, header=True, mode="overwrite")
