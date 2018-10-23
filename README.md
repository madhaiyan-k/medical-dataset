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
