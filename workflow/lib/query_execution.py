import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
       .builder \
       .appName("MEDICAL_DS_QUERIES") \
       .getOrCreate()

gpay_dataset_location = sys.argv[1]
rpay_dataset_location = sys.argv[2]

# Loading the gpay dataset into a dataframe
gpay_df = spark.read.parquet(gpay_dataset_location)

# 1st query execution and collecting the result
q1_result = gpay_df.groupBy(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty) \
                .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
                .collect()
print "Resultant row count on 1st Query: ", len(q1_result)

# 2nd query execution
df1 = gpay_df.groupBy('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID',  month('Date_of_Payment')) \
             .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
             .withColumnRenamed('month(Date_of_Payment)', 'month') \
             .withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_funding_per_month')

df2 = gpay_df.groupBy('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID') \
             .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
             .withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_funding_per_year')

condition = [df1.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name==df2.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, 
             df1.Teaching_Hospital_ID==df2.Teaching_Hospital_ID]

# collecting the result
q2_result = df1.join(df2, condition) \
            .select(df1.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,df2.Teaching_Hospital_ID,(df1.Total_funding_per_month/df2.Total_funding_per_year)*100) \
            .withColumnRenamed('((Total_funding_per_month / Total_funding_per_year) * 100)', 'funding_percentage') \
            .filter('funding_percentage >= 90') \
            .collect()
print "Resultant row count on 2nd Query: ", len(q2_result)

# Loading the rpay dataset into a dataframe
rpay_df = spark.read.parquet(rpay_dataset_location)
grouping_cols = ['Teaching_Hospital_Name',
                 'Principal_Investigator_1_Specialty',
                 'Principal_Investigator_2_Specialty',
                 'Principal_Investigator_3_Specialty',
                 'Principal_Investigator_4_Specialty',
                 'Principal_Investigator_5_Specialty']

# query execution and collecting the result
q1_result = rpay_df.groupBy(grouping_cols) \
                   .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
                   .collect()
print "Resultant row count on Query: ", len(q1_result)
