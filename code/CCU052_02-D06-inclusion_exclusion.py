# Databricks notebook source
# MAGIC %md # CCU052_02-D06-inclusion_exclusion
# MAGIC
# MAGIC **Description** This notebook creates the cohort by applying the inclusion/exclusion criteria for the project. Namely, 
# MAGIC excluding patients: aged < 0 at baseline, aged > 115 at baseline, who died before baseline, not in GDPPR, residing outside England, and failing the quality assurance. A flow diagram (table) was used to record each stage. 
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** cur_deaths_nodup, tmp_skinny, tmp_lsoa, tmp_qa
# MAGIC
# MAGIC **Data output** tmp_inc_exc, tmp_inc_exc_flow

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# MAGIC %run "../../../../shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

spark.sql(f'REFRESH TABLE {dbc}.{param_table_cur_deaths_nodup}')
spark.sql(f'REFRESH TABLE {dbc}.{param_table_tmp_skinny}')
spark.sql(f'REFRESH TABLE {dbc}.{param_table_tmp_lsoa}')
spark.sql(f'REFRESH TABLE {dbc}.{param_table_tmp_qa}')

deaths = spark.table(f'{dbc}.{param_table_cur_deaths_nodup}')
skinny = spark.table(f'{dbc}.{param_table_tmp_skinny}')
lsoa   = spark.table(f'{dbc}.{param_table_tmp_lsoa}')
qa     = spark.table(f'{dbc}.{param_table_tmp_qa}')

# COMMAND ----------

display(deaths)
display(skinny)
display(lsoa)
display(qa)

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('skinny')
print('---------------------------------------------------------------------------------')
# reduce
skinny_prepared = (
  skinny
  .select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'in_gdppr')
)

# check
count_var(skinny_prepared, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('lsoa')
print('---------------------------------------------------------------------------------')
# reduce
lsoa_prepared = (
  lsoa
  .select('PERSON_ID', 'LSOA_date', 'LSOA_conflict', 'LSOA', 'region_conflict', 'region', 'IMD_conflict', 'IMD_2019_DECILES')
  .withColumn('in_lsoa', f.lit(1))
  .withColumn('LSOA_1', f.substring(f.col('LSOA') , 1, 1))
)

# check
count_var(lsoa_prepared, 'PERSON_ID'); print()
tmpt = tab(lsoa_prepared, 'region', 'LSOA_1'); print()
tmpt = tab(lsoa_prepared.where(f.col('LSOA_1') == 'E'), 'region', 'LSOA_conflict'); print()


print('---------------------------------------------------------------------------------')
print('deaths')
print('---------------------------------------------------------------------------------')
# reduce
deaths_prepared = (
  deaths
  .select('PERSON_ID', f.col('REG_DATE_OF_DEATH').alias('DOD'))
  .withColumn('in_deaths', f.lit(1))
)

# check
count_var(deaths_prepared, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('quality assurance')
print('---------------------------------------------------------------------------------')
qa_prepared = (
  qa
  .withColumn('in_qa', f.lit(1))
)

# check
count_var(qa_prepared, 'PERSON_ID'); print()
tmpt = tab(qa_prepared, '_rule_concat', '_rule_total'); print()


print('---------------------------------------------------------------------------------')
print('sgss')
print('---------------------------------------------------------------------------------')
# not required for this project


print('---------------------------------------------------------------------------------')
print('merge')
print('---------------------------------------------------------------------------------')
# merge skinny and lsoa
tmp0 = merge(skinny_prepared, lsoa_prepared, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], keep_results=['both', 'left_only'], indicator=0); print()

# merge in deaths
tmp0 = merge(tmp0, deaths_prepared, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# merge in qa
tmp0 = merge(tmp0, qa_prepared, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()

# merge in sgss
# not required for this project 

# add study_start_date
tmp0 = (
  tmp0
  .withColumn('study_start_date', f.to_date(f.lit(study_start_date)))
)

# temp save
tmp0 = temp_save(df=tmp0, out_name=f'{proj}_tmp_inc_exc_tmp0'); print()

# check
count_var(tmp0, 'PERSON_ID'); print()
tmpt = tab(tmp0, 'study_start_date'); print()

# COMMAND ----------

# check
display(tmp0)

# check
tmpt = tab(tmp0, 'in_gdppr'); print()
tmpt = tab(tmp0, 'in_lsoa'); print()
tmpt = tab(tmp0, 'in_deaths'); print()
tmpt = tab(tmp0, 'in_qa'); print()

tmpt = tab(tmp0, '_rule_total'); print()
tmpt = tab(tmp0, '_rule_concat', '_rule_total'); print()

# tmpt = tab(_merged, 'in_sgss'); print()

# COMMAND ----------

# MAGIC %md # 3. Inclusion / exclusion

# COMMAND ----------

tmpc = count_var(tmp0, 'PERSON_ID', ret=1, df_desc='original', indx=0); print()

# COMMAND ----------

# MAGIC %md ## 3.1 Exclude patients aged < 0 at baseline

# COMMAND ----------

# filter out individuals aged < 0 at baseline (keep those >= 0 [i.e., born on or before 2019-11-01])
# 

# Include indviduals with 0 <= Age <= 115 as at 1st November 2019

study_start_date_minus_0y = str(int(study_start_date[0:4]) - 0) + study_start_date[4:]
print(f'study_start_date_minus_0y  = {study_start_date_minus_0y}'); print()

# flag
tmp0 = (
  tmp0
  .withColumn('_age_ge_0', 
              f.when(f.col('DOB').isNull(), 0)
              .when(f.col('DOB') <= f.to_date(f.lit(study_start_date_minus_0y)), 1)
              .when(f.col('DOB') >  f.to_date(f.lit(study_start_date_minus_0y)), 2)
              .otherwise(999999)
             )
  .withColumn('study_start_age', f.datediff(f.col('study_start_date'), f.col('DOB'))/365.25)
)

# check 
tmpt = tab(tmp0, '_age_ge_0'); print()
assert tmp0.where(~(f.col('_age_ge_0').isin([0,1,2]))).count() == 0
tmpt = tabstat(tmp0, 'DOB', byvar='_age_ge_0', date=1); print()
tmpt = tabstat(tmp0, 'study_start_age', byvar='_age_ge_0'); print()

# filter out and tidy
tmp1 = (
  tmp0
  .where(f.col('_age_ge_0').isin([1]))
  .drop('_age_ge_0', 'study_start_age')
)

# check
tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='post exclusion of patients aged < 0 at baseline', indx=1); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.2 Exclude patients aged > 115 at baseline

# COMMAND ----------

# filter out individuals aged > 115 (keep those <= 115 as at study baseline 2019-11-01 [i.e., born on or before xxxx])

study_start_date_minus_115y = str(int(study_start_date[0:4]) - 115) + study_start_date[4:]
print(f'study_start_date_minus_115y = {study_start_date_minus_115y}'); print()

# flag
tmp1 = (
  tmp1
  .withColumn('_age_le_115', 
              f.when(f.col('DOB').isNull(), 0)
              .when(f.col('DOB') >= f.to_date(f.lit(study_start_date_minus_115y)), 1)
              .when(f.col('DOB') <  f.to_date(f.lit(study_start_date_minus_115y)), 2)
              .otherwise(999999)
             )
  .withColumn('study_start_age', f.datediff(f.col('study_start_date'), f.col('DOB'))/365.25)
)

# check 
tmpt = tab(tmp1, '_age_le_115'); print()
assert tmp1.where(~(f.col('_age_le_115').isin([0,1,2]))).count() == 0
tmpt = tabstat(tmp1, 'DOB', byvar='_age_le_115', date=1); print()
tmpt = tabstat(tmp1, 'study_start_age', byvar='_age_le_115'); print()

# filter out and tidy
tmp2 = (
  tmp1
  .where(f.col('_age_le_115').isin([1]))
  .drop('_age_le_115', 'study_start_age')
)

# check
tmpt = count_var(tmp2, 'PERSON_ID', ret=1, df_desc='post exclusion of patients aged > 115 at baseline', indx=2); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.3 Exclude patients who died before baseline

# COMMAND ----------

# filter out patients who died before baseline

# flag
tmp2 = (
  tmp2
  .withColumn('DOD_flag', 
              f.when(f.col('DOD').isNull(), 1)
              .when(f.col('DOD') <= f.col('study_start_date'), 2)
              .when(f.col('DOD') > f.col('study_start_date'), 3)
              .otherwise(999999)
             )
)

# check 
tmpt = tab(tmp2, 'DOD_flag'); print()
assert tmp2.where(~(f.col('DOD_flag').isin([1,2,3]))).count() == 0
tmpt = tabstat(tmp2, 'DOD', byvar='DOD_flag', date=1); print()

# filter out and tidy
tmp3 = (
  tmp2
  .where(f.col('DOD_flag').isin([1,3]))
  .drop('DOD_flag')
)

# check
tmpt = count_var(tmp3, 'PERSON_ID', ret=1, df_desc='post exlusion of patients who died before study_start_date', indx=3); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.4 Exclude patients not in GDPPR

# COMMAND ----------

# check
tmpt = tab(tmp3, 'in_gdppr'); print()

# filter out patients not in GDPPR
tmp4 = tmp3.where(f.col('in_gdppr') == 1)

# check
tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='post exclusion of patients not in GDPPR', indx=4); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.5 Exclude patients with region outside of England

# COMMAND ----------

# check
tmpt = tab(tmp4, 'region', 'LSOA_1'); print()

# filter out patients with a region outside England (in Scotland and Wales)
tmp5 = tmp4.where((f.col('region').isNull()) | ((f.col('region').isNotNull()) & (~f.col('region').isin(['Scotland', 'Wales']))))

# check
tmpt = count_var(tmp5, 'PERSON_ID', ret=1, df_desc='post exclusion of patients in Scotland and Wales', indx=5); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.6 Exclude patients who failed the quality assurance

# COMMAND ----------

# Rule 1: Year of birth is after the year of death
# Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)
# Rule 3: Year of Birth Predates NHS Established Year or Year is over the Current Date
# Rule 4: Remove those with only null/invalid dates of death
# Rule 5: Remove those where registered date of death before the actual date of death
# Rule 6: Pregnancy/birth codes for men
# Rule 7: HRT codes codes for men
# Rule 8: COCP codes codes for men
# Rule 9: Prostate cancer codes for women
# Rule 10: Patients have all missing record_dates and dates

# check
tmpt = tab(tmp5, '_rule_concat', '_rule_total'); print()

# filter out patients who failed the quality assurance
tmp6 = tmp5.where(f.col('_rule_total') == 0)
  
# temp save those who failed the quality assurance for later data checks
tmp_qa_gt_0 = tmp6.where(f.col('_rule_total') > 0)
tmp_qa_gt_0 = temp_save(df=tmp_qa_gt_0, out_name=f'{proj}_tmp_inc_exc_tmp_qa_gt_0'); print()
  
# check
tmpt = count_var(tmp6, 'PERSON_ID', ret=1, df_desc='post exclusion of patients who failed the quality assurance', indx=6); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md # 4. Flow diagram

# COMMAND ----------

# check flow table
tmpp = (
  tmpc
  .orderBy('indx')
  .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
  .withColumnRenamed('df_desc', 'stage')
  .toPandas())
tmpp['n'] = tmpp['n'].astype(int)
tmpp['n_id'] = tmpp['n_id'].astype(int)
tmpp['n_id_distinct'] = tmpp['n_id_distinct'].astype(int)
tmpp['n_diff'] = (tmpp['n'] - tmpp['n'].shift(1)).fillna(0).astype(int)
tmpp['n_id_diff'] = (tmpp['n_id'] - tmpp['n_id'].shift(1)).fillna(0).astype(int)
tmpp['n_id_distinct_diff'] = (tmpp['n_id_distinct'] - tmpp['n_id_distinct'].shift(1)).fillna(0).astype(int)
for v in [col for col in tmpp.columns if re.search("^n.*", col)]:
  tmpp.loc[:, v] = tmpp[v].map('{:,.0f}'.format)
for v in [col for col in tmpp.columns if re.search(".*_diff$", col)]:  
  tmpp.loc[tmpp['stage'] == 'original', v] = ''
# tmpp = tmpp.drop('indx', axis=1)
print(tmpp.to_string()); print()

# COMMAND ----------

# MAGIC %md # 5. Prepare

# COMMAND ----------

# check
display(tmp6)

# COMMAND ----------

tmp7 = (
  tmp6
  .select('PERSON_ID', 
          'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 
          'LSOA_date', 'LSOA_conflict', 'LSOA', 'region_conflict', 'region', 'IMD_conflict', 'IMD_2019_DECILES',
          'DOD', 
          'study_start_date'
         )
)  

# check
count_var(tmp7, 'PERSON_ID')

# COMMAND ----------

# check
display(tmp7)

# COMMAND ----------

# MAGIC %md # 6. Save

# COMMAND ----------

# save
save_table(df=tmp7, out_name=f'{proj}_tmp_inc_exc', save_previous=True, dbc=dbc)
save_table(df=tmpc, out_name=f'{proj}_tmp_inc_exc_flow', save_previous=True, dbc=dbc)

# repoint
tmp7 = spark.table(f'{dbc}.{proj}_tmp_inc_exc')
tmpc = spark.table(f'{dbc}.{proj}_tmp_inc_exc_flow')