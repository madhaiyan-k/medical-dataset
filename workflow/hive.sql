
USE ${db_name};


-- Query-1:  ( List companies and break up of investment across specialties from each company )

INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Physician_Specialty,sum(Total_Amount_of_Payment_USDollars) AS total_investment FROM ${gnrl_pay_table} GROUP BY Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Physician_Specialty;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q1.csv;



-- Query-2: ( List hospitals which received funding, break up across specialities for each company )

INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  SELECT Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty,sum(Total_Amount_of_Payment_USDollars) as total_investment FROM ${rsrch_pay_table} GROUP BY Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q2.csv;



-- Query 3: ( Which states, the more funding is happening for specialty)

INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select Physician_Specialty,Recipient_State,sum(Total_Amount_of_Payment_USDollars) as Funding from ${gnrl_pay_table} group by Physician_Specialty,Recipient_State order by Funding desc limit 20;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q3.csv;




-- Query-4: ( List companies yearly investment for hospitals )

INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as Company , Teaching_Hospital_Name , payment_year , sum(Total_Amount_of_Payment_USDollars) FROM ${rsrch_pay_table} GROUP BY Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name , Teaching_Hospital_Name , payment_year;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q4.csv;





-- Query 5: ( Top doctors might be getting 80% of funding from total spend of all doctors across each specialty on a yearly basis)


INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' with c as(
    select Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as total_speciality_funding from ${gnrl_pay_table} group by Physician_Specialty,program_year
), p as(
    select Physician_Profile_ID,Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as funding_each_doctor_speciality from ${gnrl_pay_table} group by Physician_Profile_ID,Physician_Specialty,program_year
)
select
    p.Physician_Specialty,
    p.Physician_Profile_ID,
    p.program_year,
    p.funding_each_doctor_speciality,
    c.total_speciality_funding,
    ((p.funding_each_doctor_speciality/c.total_speciality_funding)*100) as percentage
from p,c
where p.Physician_Specialty=c.Physician_Specialty and p.program_year=c.program_year
order by program_year,percentage>80 asc limit 2000;


dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q5.csv;





-- Query 6: ( Joining both General and Research tables and list the common specialties and funding on a yearly basis for each of them )


INSERT OVERWRITE DIRECTORY "${output_data_dir}" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' with c as(
    select Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as gen_funding from ${gnrl_pay_table} group by Physician_Specialty,program_year
), p as(
    select Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as rsrch_funding from ${rsrch_pay_table} group by Physician_Specialty,program_year
)
select
    c.Physician_Specialty,
    c.program_year,
    (c.gen_funding+p.rsrch_funding)
from p inner join c
on p.Physician_Specialty=c.Physician_Specialty and p.program_year=c.program_year;

dfs -mv ${output_data_dir}/000000_0 ${result_bkup_dir}/q6.csv;
