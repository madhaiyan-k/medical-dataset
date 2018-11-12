use medical_data;

create table IF NOT EXISTS rsrch_pay_text(
Change_Type String,
Covered_Recipient_Type String,
Noncovered_Recipient_Entity_Name String,	
Teaching_Hospital_CCN String,	
Teaching_Hospital_ID int,	
Teaching_Hospital_Name String,	
Physician_Profile_ID int,	
Physician_First_Name String,	
Physician_Middle_Name String,	
Physician_Last_Name String,	
Physician_Name_Suffix String,	
Recipient_Primary_Business_Street_Address_Line1 String,	
Recipient_Primary_Business_Street_Address_Line2 String,	
Recipient_City String,	
Recipient_State String,	
Recipient_Zip_Code String,	
Recipient_Country String,	
Recipient_Province String,	
Recipient_Postal_Code String,	
Physician_Primary_Type String,	
Physician_Specialty String,	
Physician_License_State_code1 String,	
Physician_License_State_code2 String,	
Physician_License_State_code3 String,	
Physician_License_State_code4 String,	
Physician_License_State_code5 String,	
Principal_Investigator_1_Profile_ID int,	
Principal_Investigator_1_First_Name String,	
Principal_Investigator_1_Middle_Name String,	
Principal_Investigator_1_Last_Name String,	
Principal_Investigator_1_Name_Suffix String,	
Principal_Investigator_1_Business_Street_Address_Line1 String,	
Principal_Investigator_1_Business_Street_Address_Line2 String,	
Principal_Investigator_1_City String,	
Principal_Investigator_1_State String,	
Principal_Investigator_1_Zip_Code String,	
Principal_Investigator_1_Country String,	
Principal_Investigator_1_Province String,	
Principal_Investigator_1_Postal_Code String,	
Principal_Investigator_1_Primary_Type String,	
Principal_Investigator_1_Specialty String,	
Principal_Investigator_1_License_State_code1 String,	
Principal_Investigator_1_License_State_code2 String,	
Principal_Investigator_1_License_State_code3 String,	
Principal_Investigator_1_License_State_code4 String,	
Principal_Investigator_1_License_State_code5 String,	
Principal_Investigator_2_Profile_ID int,	
Principal_Investigator_2_First_Name String,	
Principal_Investigator_2_Middle_Name String,	
Principal_Investigator_2_Last_Name String,	
Principal_Investigator_2_Name_Suffix String,	
Principal_Investigator_2_Business_Street_Address_Line1 String,	
Principal_Investigator_2_Business_Street_Address_Line2 String,	
Principal_Investigator_2_City String,	
Principal_Investigator_2_State String,	
Principal_Investigator_2_Zip_Code String,	
Principal_Investigator_2_Country String,	
Principal_Investigator_2_Province String,	
Principal_Investigator_2_Postal_Code String,	
Principal_Investigator_2_Primary_Type String,	
Principal_Investigator_2_Specialty String,	
Principal_Investigator_2_License_State_code1 String,	
Principal_Investigator_2_License_State_code2 String,	
Principal_Investigator_2_License_State_code3 String,	
Principal_Investigator_2_License_State_code4 String,	
Principal_Investigator_2_License_State_code5 String,	
Principal_Investigator_3_Profile_ID int, 
Principal_Investigator_3_First_Name String,	
Principal_Investigator_3_Middle_Name String,	
Principal_Investigator_3_Last_Name String,	
Principal_Investigator_3_Name_Suffix String,	
Principal_Investigator_3_Business_Street_Address_Line1 String,	
Principal_Investigator_3_Business_Street_Address_Line2 String,	
Principal_Investigator_3_City String,	
Principal_Investigator_3_State String,	
Principal_Investigator_3_Zip_Code String,	
Principal_Investigator_3_Country String,	
Principal_Investigator_3_Province String,	
Principal_Investigator_3_Postal_Code String,	
Principal_Investigator_3_Primary_Type String,	
Principal_Investigator_3_Specialty String,	
Principal_Investigator_3_License_State_code1 String,	
Principal_Investigator_3_License_State_code2 String,	
Principal_Investigator_3_License_State_code3 String,	
Principal_Investigator_3_License_State_code4 String,	
Principal_Investigator_3_License_State_code5 String,	
Principal_Investigator_4_Profile_ID int, 
Principal_Investigator_4_First_Name String,	
Principal_Investigator_4_Middle_Name String,	
Principal_Investigator_4_Last_Name String,Principal_Investigator_4_Name_Suffix String,	
Principal_Investigator_4_Business_Street_Address_Line1 String,	
Principal_Investigator_4_Business_Street_Address_Line2 String,	
Principal_Investigator_4_City String,	
Principal_Investigator_4_State String,	
Principal_Investigator_4_Zip_Code String,	
Principal_Investigator_4_Country String,	
Principal_Investigator_4_Province String,	
Principal_Investigator_4_Postal_Code String,	
Principal_Investigator_4_Primary_Type String,	
Principal_Investigator_4_Specialty String,	
Principal_Investigator_4_License_State_code1 String,	
Principal_Investigator_4_License_State_code2 String,	
Principal_Investigator_4_License_State_code3 String,	
Principal_Investigator_4_License_State_code4 String,	
Principal_Investigator_4_License_State_code5 String,	
Principal_Investigator_5_Profile_ID int,	
Principal_Investigator_5_First_Name String,	
Principal_Investigator_5_Middle_Name String,	
Principal_Investigator_5_Last_Name String,	
Principal_Investigator_5_Name_Suffix String,	
Principal_Investigator_5_Business_Street_Address_Line1 String,	
Principal_Investigator_5_Business_Street_Address_Line2 String,	
Principal_Investigator_5_City String,	
Principal_Investigator_5_State String,	
Principal_Investigator_5_Zip_Code String,	
Principal_Investigator_5_Country String,	
Principal_Investigator_5_Province String,	
Principal_Investigator_5_Postal_Code String,	
Principal_Investigator_5_Primary_Type String,	
Principal_Investigator_5_Specialty String,	
Principal_Investigator_5_License_State_code1 String,	
Principal_Investigator_5_License_State_code2 String,	
Principal_Investigator_5_License_State_code3 String,	
Principal_Investigator_5_License_State_code4 String,	
Principal_Investigator_5_License_State_code5 String,	
Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name String,	
Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID String,	
Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name String,	
Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State String,	
Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country String,	
Related_Product_Indicator String,	Covered_or_Noncovered_Indicator_1 String,	
Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1 String,	
Product_Category_or_Therapeutic_Area_1 String,	
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 String,	
Associated_Drug_or_Biological_NDC_1 String,	
Covered_or_Noncovered_Indicator_2 String,	
Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2 String,	
Product_Category_or_Therapeutic_Area_2 String,	
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 String,	
Associated_Drug_or_Biological_NDC_2 String,	
Covered_or_Noncovered_Indicator_3 String,	
Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3 String,	
Product_Category_or_Therapeutic_Area_3 String,	
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 String,	
Associated_Drug_or_Biological_NDC_3 String,	
Covered_or_Noncovered_Indicator_4 String,	
Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4 String,	
Product_Category_or_Therapeutic_Area_4 String,	
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 String,	
Associated_Drug_or_Biological_NDC_4 String,	
Covered_or_Noncovered_Indicator_5 String,	
Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5 String,	
Product_Category_or_Therapeutic_Area_5 String,	
Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 String,	
Associated_Drug_or_Biological_NDC_5 String,	
Total_Amount_of_Payment_USDollars float,	
Date_of_Payment date,
Form_of_Payment_or_Transfer_of_Value String,	
Expenditure_Category1 String,	
Expenditure_Category2 String,	
Expenditure_Category3 String,	
Expenditure_Category4 String,	
Expenditure_Category5 String,	
Expenditure_Category6 String,	
Preclinical_Research_Indicator String,	
Delay_in_Publication_Indicator String,	
Name_of_Study String,	
Dispute_Status_for_Publication String,	
Record_ID String,	
Program_Year int,	
Payment_Publication_Date String,	
ClinicalTrials_Gov_Identifier String,	
Research_Information_Link String,	
Context_of_Research String) row format delimited fields terminated by "," stored as textfile tblproperties ("skip.header.line.count"="1");



load data local inpath '/tmp/sujoy_hive/output_rsrch.csv' into table rsrch_pay_text;
