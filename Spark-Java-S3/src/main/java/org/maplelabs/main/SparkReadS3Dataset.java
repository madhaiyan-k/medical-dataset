/*
 * Copyright (c) 2018 MapleLabs. All Rights Reserved.
 */

package org.maplelabs.main;

import org.apache.log4j.Logger;
import org.apache.hadoop.security.AccessControlException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * Reads the dataset from AWS S3 bucket
 */
public class SparkReadS3Dataset {
    private static final Logger LOGGER = Logger.getLogger(SparkReadS3Dataset.class);
    public static void main(String[] args) throws SecurityException, IOException {
        if (args.length != 2) {
            LOGGER.error("Please provide the correct number of arguments...");
            System.exit(1);
        }
        SparkSession spark = SparkSession.builder().appName("Spark_S3")
                                                   .config("fs.s3n.awsAccessKeyId", args[0])
                                                   .config("fs.s3n.awsSecretAccessKey", args[1])
                                                   .getOrCreate();
        String awsContainer = "s3n://maplelabs/maple";
        String gnrlPayPath = awsContainer + "/OP_DTL_GNRL_PAY.csv";
        String rsrchPayPath = "s3n://maplelabs/maple/OP_DTL_RSRCH_PAY.csv";
        try {
            LOGGER.info("Reading the csv file" + gnrlPayPath + "from S3 bucket");
            Dataset<Row> s3aRdd1 = spark.read().format("csv")
                                               .option("header", "true")
                                               .option("treatEmptyValuesAsNulls", "true")
                                               .option("nullValue", "0")
                                               .option("sep", ",")
                                               .csv(gnrlPayPath);
            s3aRdd1.createOrReplaceTempView("gnrl_pay_orc");
            s3aRdd1.createOrReplaceTempView("a");
            s3aRdd1.createOrReplaceTempView("b");
	    if (s3aRdd1.count() > 0) {
                LOGGER.info("Number of records in the given file is " + s3aRdd1.count());
                s3aRdd1.createOrReplaceTempView("temp_table");
                s3aRdd1.createOrReplaceTempView("gnrl_pay_orc");
                String query1 = "select count(physician_profile_id) as no_of_physicians,Recipient_Zip_" +
                                "Code from temp_table group by Recipient_Zip_Code";
                String query2 = "select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name," +
                                "Physician_Specialty,sum(Total_Amount_of_Payment_USDollars) as total_i" +
                                "nvestment from gnrl_pay_orc group by Applicable_Manufacturer_or_Appli" +
                                "cable_GPO_Making_Payment_Name,Physician_Specialty";
                String query3 = "select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name," +
                                "Teaching_Hospital_ID,month(Date_of_Payment) as month, sum(Total_Amoun" + 
                                "t_of_Payment_USDollars) as total_funding_per_month from gnrl_pay_orc " + 
                                "group by Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Nam" +
                                "e,Teaching_Hospital_ID,month(Date_of_Payment)";
                String query4 = "select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name," +
                                "Teaching_Hospital_ID, sum(Total_Amount_of_Payment_USDollars) as total" +
                                "_funding_per_year from gnrl_pay_orc group by Applicable_Manufacturer_" +
                                "or_Applicable_GPO_Making_Payment_Name,Teaching_Hospital_ID";
               
                // Number of physicians group by Recipient_Zip_Code
                Dataset<Row> df1 = spark.sql(query1) ;
                df1.show();
                LOGGER.info("Computation completed for 1 query");

                // List companies & break up of investment across specialities
                Dataset<Row> df2 = spark.sql(query2) ;
                df2.show();
                LOGGER.info("Computation completed for 2 queries");

                // what is funding the company did for multiple years (how does that change)
                // for each hospital, list companies with funding more than 90%
                Dataset<Row> a = spark.sql(query3);
                Dataset<Row> b = spark.sql(query4);
                Dataset <Row> joined = a.join(b, a.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name")
                                        .equalTo(b.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"))
                                        .and(a.col("Teaching_Hospital_ID")
                                        .equalTo(b.col("Teaching_Hospital_ID"))));
                String temp = "(a.col(\"total_funding_per_month\").divide(b.col(\"total_funding_per_year\"))).multiply(100)";
                Dataset <Row> joinFilter = joined.select(a.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"),
                                           b.col("Teaching_Hospital_ID"), a.col("total_funding_per_month").divide(b.col("total_funding_per_year"))
                                                                                                          .multiply(100));
                Dataset <Row> joinFilter2 = joinFilter.withColumnRenamed("((total_funding_per_month / total_funding_per_year) * 100)",
                                                                         "funding_percentage");
                Dataset <Row> joinFilter3 = joinFilter2.filter("funding_percentage >= 90");
                joinFilter3.show();
                LOGGER.info("Computation completed for 3 queries");
	    } else {
                LOGGER.error("No records found in " + gnrlPayPath);
                System.exit(1);
	    }
            LOGGER.info("Reading the csv file" + rsrchPayPath + "from S3 bucket");
            Dataset<Row> s3aRdd2 = spark.read().format("csv")
                                               .option("header", "true")
                                               .option("treatEmptyValuesAsNulls", "true")
                                               .option("nullValue", "0")
                                               .option("sep", ",")
                                               .csv(rsrchPayPath);
            if (s3aRdd2.count() > 0) {
                LOGGER.info("Number of records in the given file is " + s3aRdd2.count());
                s3aRdd2.createOrReplaceTempView("rsrch_pay_orc");
                // list hospitals which received funding, break up across specialities
                // for each company, which hospitals/specialities does it fund
                String query5 = "select Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Prin" +
                                "cipal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Pri" +
                                "ncipal_Investigator_4_Specialty,Principal_Investigator_5_Specialty,su" +
                                "m(Total_Amount_of_Payment_USDollars) as total_investment from rsrch_p" +
                                "ay_orc group by Teaching_Hospital_Name,Principal_Investigator_1_Speci" +
                                "alty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Spec" +
                                "ialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Spe" +
                                "cialty";
                Dataset<Row> df3 = spark.sql(query5) ;
                df3.show();
                LOGGER.info("Computation completed for 4 queries");
            } else {
                LOGGER.error("No records found in " + rsrchPayPath);
                System.exit(1);
            }
        } catch (Exception ex) {
            LOGGER.error(ex.toString());
            System.exit(1);
        }
    }
}
