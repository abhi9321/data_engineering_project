def load_data(transformed_df, output_path):
    # output_path = r"C:\Users\4906031\PycharmProjects\data_engineering_project\data\final\output.csv"
    transformed_df.write.csv(output_path, header=True, mode="overwrite")
