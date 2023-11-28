import pandas as pd

def chunk_merge_to_csv(df1_path, df2, output_csv_file, chunksize=10000):

    # Open the output file in 'write' mode to clear any existing content
    with open(output_csv_file, 'w', newline='') as output_file:
        # Write an empty header to the file
        pd.DataFrame().to_csv(output_file, index=False)

    # Open the output file in 'append' mode
    with open(output_csv_file, 'a', newline='') as output_file:
        # Iterate through chunks of the first DataFrame
        for chunk_df1 in pd.read_csv(df1_path, chunksize=chunksize):
            # Merge each chunk with the second DataFrame
            merged_chunk = pd.merge(chunk_df1, df2, on="sk_id_curr", how='outer')
            
            # Append the result to the CSV file
            merged_chunk.to_csv(output_file, index=False, header=not output_file.tell())

# Specify the output CSV file
output_csv_file = 'merged_output.csv'

# Specify the paths to your input DataFrames
loan_application_path = "Dataset/loan_applications_test.csv" 
previous_credits = pd.read_csv("Dataset/previous_credits.csv")

# Perform the chunk merge and write the results directly to the CSV file
chunk_merge_to_csv(loan_application_path, previous_credits, output_csv_file)

# Now you can read the merged results from the CSV file if needed
merged_result = pd.read_csv(output_csv_file)
print(merged_result.head())
