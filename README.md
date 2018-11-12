# medical-dataset

It runs a workflow for hive queries.

Prerequisite:
Create data base and tables for medical dataset before running workflow.

Update following configuration before you run workflow for scheduling mode.

1. Configure NameNode URL and Job Track in coordinator.properties
2. Configure data base and table names in coordinator.properties
3. Update oozie command (oozie job --oozie http://manager-0:11000/oozie -config coordinator.properties -run) in setup.sh
4. Run setup.sh for schedule mode workflow execution

Update following configuration before you run workflow for one time.

1. Configure NameNode URL and Job Track in job.properties
2. Configure data base and table names in job.properties
3. Update oozie command (oozie job --oozie http://manager-0:11000/oozie -config job.properties -run) in setup.sh
4. Run setup.sh for workflow execution


## spark pre-requisites
Convert the plain dataset into parquet file using snappy compression for better results (default in spark2.3).
See spark_jobs directory for more information

By running this oozie job, will met into an issue of jackson dependancy with oozie and spark.
https://community.hortonworks.com/content/supportkb/186305/error-comfasterxmljacksondatabindjsonmappingexcept.html

To avoid that, please follow the solution provided in the above post.



## HIVE


### 1. Data Preparation and Cleansing 
	
	a. Download the raw CSV files from https://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads.html 
	
	b. Unzip the file , there will be three files viz. General payment,Research Payment,Ownership Payment .
	
	c. Process the raw CSV ( General and Research ) file using the gnrl_process.py and rsrch_process.py in scripts folder .
	
	d. Run the Python file in this format " python gnrl_process.py/rsrch_process.py < raw_csv_file > <output_csv_file> " i.e 	arg1="raw csv filename" arg2="output csv filename"
	
	e. Here , we will be cahnging the "Date_Of_Payment" column values to default timestamp structure i.e "YYYY-MM-DD" 
	   and also remove comma within Double quotes else Hive will treat them as seperate fields .


### 2. Data Loading

	a. To load data we will be creating EXTERNAL TABLES for each year dataset ( For both General and Research ) and merge it to a single INTERNAL TABLE . 
	
	b. Copy all the processed csv Files from Local filesystem to HDFS ( hdfs dfs -put ........... ) in location "/user/root/medical_data/gnrl_data/ and "/user/root/medical_data/rsrch_data/ "
	
	c. Run gnrl_5_year_loading_data.sql( General_Pay table ) and rsrch_5_year_loading_data.sql ( Research_pay table ) to create the Hive Tables with whole 5 year data.

### 3. Querying Data
	
	a. Run the hive_queries.sql file .

#### Note:

1. To run hive sql files use command " hive -f < file_name.sql > "
2. Data while loaded is being partitioned on year and month on a quarterly basis .


