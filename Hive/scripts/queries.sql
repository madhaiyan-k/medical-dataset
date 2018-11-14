use medical_data_new;

set hive.execution.engine=tez;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;
set hive.optimize.index.filter=true;
set hive.fetch.task.conversion=more;
set hive.exec.parallel=true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.tez.container.size=2048;
set hive.auto.convert.join.noconditionaltask.size=3000;


/* Query-1:  ( List companies and break up of investment across specialties from each company ) */

select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Physician_Specialty,sum(Total_Amount_of_Payment_USDollars) as total_investment from gnrl_pay_orc_partition_3 group by Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Physician_Specialty;


/* Query-2: ( List hospitals which received funding, break up across specialities for each company ) */

select Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty,sum(Total_Amount_of_Payment_USDollars) as total_investment from rsrch_pay_orc group by Teaching_Hospital_Name,Principal_Investigator_1_Specialty,Principal_Investigator_2_Specialty,Principal_Investigator_3_Specialty,Principal_Investigator_4_Specialty,Principal_Investigator_5_Specialty;


/* Query 3: ( Which states, the more funding is happening for specialty)  */

select Physician_Specialty,Recipient_State,sum(Total_Amount_of_Payment_USDollars) as Funding from gnrl_pay_orc_partition_3 group by Physician_Specialty,Recipient_State order by Funding desc limit 20;


/* Query-4: ( List companies yearly investment for hospitals )  */

select Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as Company , Teaching_Hospital_Name , payment_year , sum(Total_Amount_of_Payment_USDollars) FROM rsrch_pay_orc_partition_3 GROUP BY Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name , Teaching_Hospital_Name , payment_year;



/* Query 5: ( Top doctors might be getting 80% of funding from total spend of all doctors across each specialty on a yearly basis) */


with c as(
    select Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as total_speciality_funding from gnrl_pay_orc_partition_3 group by Physician_Specialty,program_year
), p as(
    select Physician_Profile_ID,Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as funding_each_doctor_speciality from gnrl_pay_orc_partition_3 group by Physician_Profile_ID,Physician_Specialty,program_year
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


/* Query 6: ( Joining Both General and Research Table and finding the common specialities and total funding for each of them ) */

with c as(
    select Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as gen_funding from gnrl_pay_orc_partition_3 group by Physician_Specialty,program_year
), p as(
    select Physician_Specialty,program_year,sum(Total_Amount_of_Payment_USDollars) as rsrch_funding from rsrch_pay_orc_partition_3 group by Physician_Specialty,program_year
)
select
    c.Physician_Specialty,
    c.program_year,
    (c.gen_funding+p.rsrch_funding)
from p inner join c
on p.Physician_Specialty=c.Physician_Specialty and p.program_year=c.program_year;
