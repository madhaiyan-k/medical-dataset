# Preparing the plain dataset to execute queries

Working on 6GB plain text file will take so much time to process the input query

The above spark code will create a compressed (6GB to ~340MB) parquet file after organized the data into an proper dataframe (Converting from string columns to int,float and date columns)

`Example Usage: <spark_submit_with_necessary_args> prepare_gpay_ds.py <input_plain_dataset_location> <output_parquet_location>`
