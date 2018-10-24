
USE ${db_name};

INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Physician_Specialty,sum(Total_Amount_of_Payment_USDollars) AS total_investment FROM ${gnrl_pay_table} GROUP BY Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Physician_Specialty;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q1.csv;


INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  SELECT Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty,sum(Total_Amount_of_Payment_USDollars) as total_investment FROM ${rsrch_pay_table} GROUP BY Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q2.csv;


INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  SELECT Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as Company , Teaching_Hospital_Name , payment_month , sum(Total_Amount_of_Payment_USDollars) FROM ${rsrch_pay_table} GROUP BY Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name , Teaching_Hospital_Name , payment_month;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q3.csv;
