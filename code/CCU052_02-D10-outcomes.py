# Databricks notebook source
# MAGIC %md # CCU052_02-D10-outcomes
# MAGIC
# MAGIC **Description** This notebook creates the outcomes for the project, which are defined with respect to the study start date using the project codelist together with GDPPR, HES APC, and Deaths. 
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** codelist_outcomes, out_cohort, gdppr, cur_hes_apc_long, cur_deaths_long
# MAGIC
# MAGIC **Data output** out_outcomes

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

#import the pyspaprk module
from pyspark.sql.functions import col,lit,when

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

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

spark.sql(f'REFRESH TABLE {dbc}.{param_table_codelist_outcomes}')
codelist_out  = spark.table(f'{dbc}.{param_table_codelist_outcomes}')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort       = spark.table(f'{dbc}.{proj}_out_cohort')

gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

spark.sql(f'REFRESH TABLE {dbc}.{param_table_cur_hes_apc_long}')
hes_apc_long = spark.table(f'{dbc}.{param_table_cur_hes_apc_long}')

spark.sql(f'REFRESH TABLE {dbc}.{param_table_cur_deaths_nodup_long}')
deaths_long  = spark.table(f'{dbc}.{param_table_cur_deaths_nodup_long}')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('codelist')
print('--------------------------------------------------------------------------------------')
# check
tmpt = tab(codelist_out, 'name', 'terminology'); print()

# _codelist_eo = codelist_eo.where(f.col('name').isin(['AF', 'hypertension', 'hypertension_drugs']))

# check
tmpt = tab(codelist_out, 'name', 'terminology'); print()
print(codelist_out.limit(10).toPandas().to_string()); print()

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
# check
print(f'study_end_date = {study_end_date}')
assert study_end_date == '2023-06-30'

individual_censor_dates = (
  cohort
  .select('PERSON_ID', f.col('study_start_date').alias('CENSOR_DATE_START'))
  .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{study_end_date}'))))

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# exclude CODE/PRACTICE/DATE combinations where it appears that a code has been applied to all patients registered at the practice
# see notebook: data_checks/...codelist_check_practice for further details

# COMMAND ----------

# check
count_gdppr = gdppr.count()
print(f'count_gdppr = {count_gdppr:,}'); print()

# --------------------------------------------------------------------------------------------
# separate instance_1
# --------------------------------------------------------------------------------------------
# drop instance_1
gdppr_filtered_1 = (
  gdppr
  .where(~(instance_1))
) 
count_gdppr_filtered_1 = gdppr_filtered_1.count()
print(f'count_gdppr_filtered_1 = {count_gdppr_filtered_1:,}'); print()

# keep instance_1 only
gdppr_instance_1 = (
  gdppr
  .where(instance_1)
)
count_gdppr_instance_1 = gdppr_instance_1.count()
print(f'count_gdppr_instance_1 = {count_gdppr_instance_1:,}'); print()

# check
if(count_gdppr != count_gdppr_filtered_1 + count_gdppr_instance_1):
  print('WARNING: count_gdppr !=  count_gdppr_filtered_1 +  count_gdppr_instance_1'); print()
else:
  print('INFO: count_gdppr ==  count_gdppr_filtered_1 +  count_gdppr_instance_1'); print()

# --------------------------------------------------------------------------------------------
# get instance_1 PERSON_ID, DATE, CODE
# --------------------------------------------------------------------------------------------
gdppr_instance_1_tmp = (
  gdppr_instance_1
  .select('NHS_NUMBER_DEID', 'DATE', 'CODE')
  .distinct()
  .withColumn('instance_1', f.lit(1))
)
gdppr_instance_1_tmp.cache().count()
count_gdppr_instance_1_tmp = gdppr_instance_1_tmp.count()
print(f'count_gdppr_instance_1_tmp = {count_gdppr_instance_1_tmp:,}'); print()

# --------------------------------------------------------------------------------------------
# flag instance_1 PERSON_ID, DATE, CODE
# --------------------------------------------------------------------------------------------
gdppr_filtered_2 = (
  gdppr_filtered_1
  .join(f.broadcast(gdppr_instance_1_tmp), on=['NHS_NUMBER_DEID', 'DATE', 'CODE'], how='left')
) 
count_gdppr_filtered_2 = gdppr_filtered_2.count()
print(f'count_gdppr_filtered_2 = {count_gdppr_filtered_2:,}'); print()

# check
if(count_gdppr_filtered_1 != count_gdppr_filtered_2):
  print('WARNING: count_gdppr_filtered_1 != count_gdppr_filtered_2'); print()
else:
  print('INFO: count_gdppr_filtered_1 == count_gdppr_filtered_2'); print()

# check
count_gdppr_filtered_2_instance_1 = gdppr_filtered_2.where(f.col('instance_1') == 1).count()
print(f'count_gdppr_filtered_2_instance_1 = {count_gdppr_filtered_2_instance_1:,}'); print()

# --------------------------------------------------------------------------------------------
# remove instance_1 PERSON_ID, DATE, CODE that have been copied across to another practice
# --------------------------------------------------------------------------------------------
gdppr_filtered_3 = (
  gdppr_filtered_2
  .where(f.col('instance_1').isNull())
)
count_gdppr_filtered_3 = gdppr_filtered_3.count()
print(f'count_gdppr_filtered_3 = {count_gdppr_filtered_3:,}'); print()

# check
if(count_gdppr_filtered_2 != count_gdppr_filtered_2_instance_1 + count_gdppr_filtered_3):
  print('WARNING: count_gdppr_filtered_2 != count_gdppr_filtered_2_instance_1 + count_gdppr_filtered_3'); print()
else:
  print('INFO: count_gdppr_filtered_2 == count_gdppr_filtered_2_instance_1 + count_gdppr_filtered_3'); print()




# COMMAND ----------

# --------------------------------------------------------------------------------------------
# separate instance_2
# --------------------------------------------------------------------------------------------
# drop instance_2
gdppr_filtered_4 = (
  gdppr_filtered_3
  .where(~(instance_2))
) 
count_gdppr_filtered_4 = gdppr_filtered_4.count()
print(f'count_gdppr_filtered_4 = {count_gdppr_filtered_4:,}'); print()

# keep instance_2 only
gdppr_instance_2 = (
  gdppr_filtered_3
  .where(instance_2)
)
count_gdppr_instance_2 = gdppr_instance_2.count()
print(f'count_gdppr_instance_2 = {count_gdppr_instance_2:,}'); print()

# check
if(count_gdppr_filtered_3 != count_gdppr_filtered_4 + count_gdppr_instance_2):
  print('WARNING: count_gdppr_filtered_3 !=  count_gdppr_filtered_4 +  count_gdppr_instance_2'); print()
else:
  print('INFO: count_gdppr_filtered_3 == count_gdppr_filtered_4 + count_gdppr_instance_2'); print()

# --------------------------------------------------------------------------------------------
# get instance_2 PERSON_ID, DATE, CODE
# --------------------------------------------------------------------------------------------
gdppr_instance_2_tmp = (
  gdppr_instance_2
  .select('NHS_NUMBER_DEID', 'DATE', 'CODE')
  .distinct()
  .withColumn('instance_2', f.lit(1))
)
gdppr_instance_2_tmp.cache().count()
count_gdppr_instance_2_tmp = gdppr_instance_2_tmp.count()
print(f'count_gdppr_instance_2_tmp = {count_gdppr_instance_2_tmp:,}'); print()

# --------------------------------------------------------------------------------------------
# flag instance_2 PERSON_ID, DATE, CODE
# --------------------------------------------------------------------------------------------
gdppr_filtered_5 = (
  gdppr_filtered_4
  .join(f.broadcast(gdppr_instance_2_tmp), on=['NHS_NUMBER_DEID', 'DATE', 'CODE'], how='left')
) 
count_gdppr_filtered_5 = gdppr_filtered_5.count()
print(f'count_gdppr_filtered_5 = {count_gdppr_filtered_5:,}'); print()

# check
if(count_gdppr_filtered_4 != count_gdppr_filtered_5):
  print('WARNING: count_gdppr_filtered_4 != count_gdppr_filtered_5'); print()
else:
  print('INFO: count_gdppr_filtered_4 == count_gdppr_filtered_5'); print()

# check
count_gdppr_filtered_5_instance_2 = gdppr_filtered_5.where(f.col('instance_2') == 1).count()
print(f'count_gdppr_filtered_5_instance_2 = {count_gdppr_filtered_5_instance_2:,}'); print()

# --------------------------------------------------------------------------------------------
# remove instance_1 PERSON_ID, DATE, CODE that have been copied across to another practice
# --------------------------------------------------------------------------------------------
gdppr_filtered_6 = (
  gdppr_filtered_5
  .where(f.col('instance_2').isNull())
)
count_gdppr_filtered_6 = gdppr_filtered_6.count()
print(f'count_gdppr_filtered_6 = {count_gdppr_filtered_6:,}'); print()

# check
if(count_gdppr_filtered_5 != count_gdppr_filtered_5_instance_2 + count_gdppr_filtered_6):
  print('WARNING: count_gdppr_filtered_5 != count_gdppr_filtered_5_instance_2 + count_gdppr_filtered_6'); print()
else:
  print('INFO: count_gdppr_filtered_5 == count_gdppr_filtered_5_instance_2 + count_gdppr_filtered_6'); print()



# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
gdppr_prepared = (
  gdppr_filtered_6
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# add individual censor dates
gdppr_prepared = (
  gdppr_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
gdppr_prepared = (
  gdppr_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# temp save (checkpoint)
gdppr_prepared = temp_save(df=gdppr_prepared, out_name=f'{proj}_tmp_outcomes_gdppr'); print()

# check
count_var(gdppr_prepared, 'PERSON_ID'); print()
tmpt = tabstat(gdppr_prepared, 'DATE', date=1); print()
print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# COMMAND ----------

gdppr_prepared = spark.table(f'{dbc}.{proj}_tmp_outcomes_gdppr')

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
tmpt = tab(hes_apc_long, 'DIAG_POSITION'); print()

# primary diagnostic position only
# reduce and rename columns
hes_apc_prepared = (
  hes_apc_long
  .where(f.col('DIAG_POSITION') == '1')
  .select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check
# count_var(hes_apc_prepared, 'PERSON_ID'); print()
tmpt = tab(hes_apc_prepared, 'DIAG_POSITION'); print()

# add individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_prepared = (
  hes_apc_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(hes_apc_prepared, 'PERSON_ID'); print()

# # filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
hes_apc_prepared = (
  hes_apc_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# temp save (checkpoint)
hes_apc_prepared = temp_save(df=hes_apc_prepared, out_name=f'{proj}_tmp_outcomes_hes_apc')

# check
count_var(hes_apc_prepared, 'PERSON_ID'); print()
tmpt = tabstat(hes_apc_prepared, 'DATE', date=1); print()
print(hes_apc_prepared.limit(10).toPandas().to_string()); print()

# COMMAND ----------

hes_apc_prepared = spark.table(f'{dbc}.{proj}_tmp_outcomes_hes_apc').drop('DIAG_POSITION', 'DIAG_DIGITS')

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('deaths')
print('--------------------------------------------------------------------------------------')
tmpt = tab(deaths_long, 'DIAG_POSITION'); print()

# primary diagnostic position only
# reduce and rename columns
deaths_prepared = (
  deaths_long
  .where(f.col('DIAG_POSITION') == 'UNDERLYING')
  .select('PERSON_ID', 'DATE', 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check
# count_var(deaths_prepared, 'PERSON_ID'); print()
tmpt = tab(deaths_prepared, 'DIAG_POSITION'); print()

# add individual censor dates
# deaths_prepared = merge(deaths_prepared, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
deaths_prepared = (
  deaths_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(deaths_prepared, 'PERSON_ID'); print()

# # filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
deaths_prepared = (
  deaths_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# temp save (checkpoint)
deaths_prepared = temp_save(df=deaths_prepared, out_name=f'{proj}_tmp_outcomes_deaths')

# check
count_var(deaths_prepared, 'PERSON_ID'); print()
tmpt = tabstat(deaths_prepared, 'DATE', date=1); print()
print(deaths_prepared.limit(10).toPandas().to_string()); print()

# COMMAND ----------

deaths_prepared = spark.table(f'{dbc}.{proj}_tmp_outcomes_deaths').drop('DIAG_POSITION', 'DIAG_DIGITS')

# COMMAND ----------

# MAGIC %md # 3 Codelist match

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# check
display(codelist_out)

# COMMAND ----------

# check
tmpt = tab(codelist_out, 'name', 'terminology'); print()

# COMMAND ----------

# separate codelists
print('codelist_out_icd10_hes_apc\n')
codelist_out_icd10_hes_apc = (
  codelist_out
  .where(f.col('terminology') == 'ICD10')
  .withColumn('name', f.concat(f.col('name'), f.lit('_hes_apc')))
)
tmpt = tab(codelist_out_icd10_hes_apc, 'name', 'terminology'); print()
# print(codelist_out_icd10_hes_apc.orderBy('name', 'code').toPandas().to_string()); print()
display(codelist_out_icd10_hes_apc)


print('codelist_out_icd10_death\n')
codelist_out_icd10_deaths = (
  codelist_out
  .where(f.col('terminology') == 'ICD10')
  .withColumn('name', f.concat(f.col('name'), f.lit('_death')))
)
tmpt = tab(codelist_out_icd10_deaths, 'name', 'terminology'); print()
# print(codelist_out_icd10_deaths.orderBy('name', 'code').toPandas().to_string()); print()
display(codelist_out_icd10_deaths)


print('codelist_out_snomed\n')
codelist_out_snomed = (
  codelist_out
  .where(f.col('terminology') == 'SNOMED')
  .withColumn('name', f.concat(f.col('name'), f.lit('_gdppr')))
)
tmpt = tab(codelist_out_snomed, 'name', 'terminology'); print()
# print(codelist_out_snomed.orderBy('name', 'code').toPandas().to_string()); print()
display(codelist_out_snomed)

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_out = {
    'gdppr':    ['gdppr_prepared',    'codelist_out_snomed', 2]
  , 'hes_apc':  ['hes_apc_prepared',  'codelist_out_icd10_hes_apc', 1]
  , 'deaths':   ['deaths_prepared',   'codelist_out_icd10_deaths', 3]
}

# run codelist match and codelist match summary functions
out, out_1st, out_1st_wide = codelist_match(dict_out, _name_prefix=f'out_')
out_summ_name, out_summ_name_code = codelist_match_summ(dict_out, out)

# temp save
out_all = out['all']
out_all = temp_save(df=out_all, out_name=f'{proj}_tmp_outcomes_out_all'); print()
out_1st = temp_save(df=out_1st, out_name=f'{proj}_tmp_outcomes_out_1st'); print()
out_1st_wide = temp_save(df=out_1st_wide, out_name=f'{proj}_tmp_outcomes_out_1st_wide'); print()
out_summ_name = temp_save(df=out_summ_name, out_name=f'{proj}_tmp_outcomes_out_summ_name'); print()
out_summ_name_code = temp_save(df=out_summ_name_code, out_name=f'{proj}_tmp_outcomes_out_summ_name_code'); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

display(deaths_long.where(f.col('PERSON_ID') == ''))
#display(deaths_prepared.where(f.col('PERSON_ID') == ''))

# COMMAND ----------

display(out_all.where(f.col('code') == 'J46').where(f.col('source') == 'deaths'))

# COMMAND ----------

count_var(out_1st_wide, 'PERSON_ID')

# COMMAND ----------

# check result
display(out_1st_wide)

# COMMAND ----------

# check codelist match summary by name and source
display(out_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(out_summ_name_code)

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# save
save_table(df=out_1st_wide, out_name=f'{proj}_out_outcomes', save_previous=True, dbc=dbc)

# repoint
out_1st_wide = spark.table(f'{dbc}.{proj}_out_outcomes')