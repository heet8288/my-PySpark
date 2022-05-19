-- Databricks notebook source
/* Table crated clinicaltrial_2021 */
CREATE EXTERNAL TABLE if not exists clinicaltrial_2021(
ID STRING,
Sponsor STRING,
Status STRING,
Start STRING,
Completion STRING,
Type STRING,
Submission STRING,
Conditions STRING,
Interventions STRING)
USING csv OPTIONS ( 'multiLine' 'true', 'escape' '"', 'header' 'true', 'delimiter' '|')
LOCATION '/FileStore/tables/clinicaltrial_2021.csv';

-- COMMAND ----------

/* Table crated pharma */
CREATE TABLE if not exists pharma(
Company STRING,
Parent_Company STRING,
Penalty_Amount STRING,
Subtraction_From_Penalty STRING,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
Penalty_Year STRING,
Penalty_Date STRING,
Offense_Group STRING,
Primary_Offense STRING,
Secondary_Offense STRING,
Description STRING,
Level_of_Government STRING,
Action_Type STRING,
Agency STRING,
Civil_Criminal STRING,
Prosecution_Agreement STRING,
Court STRING,
Case_ID STRING,
Private_Litigation_Case_Title STRING,
Lawsuit_Resolution STRING,
Facility_State STRING,
City STRING,
Address STRING,
Zip STRING,
NAICS_Code STRING,
NAICS_Translation STRING,
HQ_Country_of_Parent STRING,
HQ_State_of_Parent STRING,
Ownership_Structure STRING,
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING,
Specific_Industry_of_Parent STRING,
Info_Source STRING,
Notes STRING)
USING csv OPTIONS ( 'multiLine' 'true', 'escape' '"', 'header' 'true', 'delimiter' ',')
LOCATION '/FileStore/tables/pharma.csv';

-- COMMAND ----------

/* Table created mesh */
create table if not exists mesh(
term STRING,
tree STRING)
USING csv OPTIONS ( 'multiLine' 'true', 'escape' '"', 'header' 'true', 'delimiter' ',')
LOCATION '/FileStore/tables/mesh.csv';

-- COMMAND ----------

/* Question -1 */
select distinct count(*) from clinicaltrial_2021;

-- COMMAND ----------

/* Question -2 */
select Type,count(Type) from clinicaltrial_2021 group by Type order by count(Type) desc

-- COMMAND ----------

/* Question -3 */
create view if not exists condtion_view as select explode(split(conditions,',')) as condition from clinicaltrial_2021 

-- COMMAND ----------

/* Question -3 */
select condition,count(condition) as count from condtion_view where condition != "" group by condition order by count desc limit 5

-- COMMAND ----------

/* Question -4 */
create view if not exists mesh_split as select term,SPLIT(tree,'[\.]')[0] as tree from mesh

-- COMMAND ----------

/* Question -4 */
select tree,count(tree) as count from condtion_view c inner join mesh_split m on m.term == c.condition group by tree order by count desc limit 10

-- COMMAND ----------

/* Question -5 */
select sponsor, count(sponsor) as count 
from clinicaltrial_2021 where sponsor not in (select parent_company from pharma) 
group by sponsor order by count desc limit 10;

-- COMMAND ----------

/* Question -6 */
select split(Completion," ")[0] as month,count(split(Completion," ")[0]) as count 
from clinicaltrial_2021 where status="Completed" and split(Completion," ")[1] == "2021"
group by month order by count desc
