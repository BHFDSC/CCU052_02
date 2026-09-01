# Databricks notebook source
# MAGIC %md # CCU052_02-D08-covariates
# MAGIC
# MAGIC **Description** This notebook creates the covariates for the project, which are defined with respect to the study start date using the project codelist together with GDPPR and HES APC. The Charlson Comorbidity Index was also derived. 
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** codelist_out, codelist_char, out_cohort, gdppr, cur_hes_apc_long
# MAGIC
# MAGIC **Data output** out_covariates

# COMMAND ----------

spark.sql('CLEAR CACHE')

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

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

codelist_out  = spark.table(f'{dbc}.{param_table_codelist_outcomes}')
codelist_char = spark.table(f'{dbc}.{param_table_codelist_charlson}')

spark.sql(f'REFRESH TABLE {dbc}.{param_table_out_cohort}')
cohort        = spark.table(f'{dbc}.{param_table_out_cohort}')

gdppr         = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

spark.sql(f'REFRESH TABLE {dbc}.{param_table_cur_hes_apc_long}')
hes_apc_long  = spark.table(f'{dbc}.{param_table_cur_hes_apc_long}')

# COMMAND ----------

cci_weight_dic = {
    "mi": 1,
    "chf": 1,
    "pad": 1,
    "cevd": 1,
    "dementia": 1,
    "cpd": 1,
    "rheumd": 1,
    "pud": 1,
    "diabwc": 2,
    "hp": 2,
    "rend": 2,
    "sld": 3,
    "metacanc": 6,
    "hiv": 6,
    "diab": 1,
    "canc": 2,
    "mld": 1
}

# check
len(cci_weight_dic) # 17

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
print(f'study_start_date = {study_start_date}')
individual_censor_dates = (
  cohort
  .select('PERSON_ID', 'DOB')
  .withColumnRenamed('DOB', 'CENSOR_DATE_START')
  .withColumn('CENSOR_DATE_END', f.to_date(f.lit(f'{study_start_date}')))
)

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
gdppr_prepared = (
  gdppr
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

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()
# print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
gdppr_prepared = temp_save(df=gdppr_prepared, out_name=f'{proj}_tmp_covariates_gdppr')

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
hes_apc_long_prepared = (
  hes_apc_long
  .select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check 1
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check 2
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)
# tmpt = tab(hes_apc_long_prepared, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_1').isin([2]))
  .drop('flag_1')
)

# check 3
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
)
# tmpt = tab(hes_apc_long_prepared, 'flag_2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_2').isin([1]))
  .drop('flag_2')
)

# check 4
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()
# print(hes_apc_long_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
hes_apc_long_prepared = temp_save(df=hes_apc_long_prepared, out_name=f'{proj}_tmp_covariates_hes_apc')

# COMMAND ----------

gdppr_prepared = spark.table(f'{dbc}.{proj}_tmp_covariates_gdppr')
hes_apc_long_prepared = spark.table(f'{dbc}.{proj}_tmp_covariates_hes_apc')

# COMMAND ----------

# # check
# tmp1 = (
#   gdppr_prepared
#   .where(f.col('CODE').isin(['63480004']))
# )
# count_var(tmp1, 'PERSON_ID')
# tmp2 = (
#   gdppr_prepared
#   .where(f.col('CODE').isin(['407674008']))
# )
# count_var(tmp2, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 3 HX Outcomes

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# check
display(codelist_out)

# COMMAND ----------

# check
tmpt = tab(codelist_out, 'name', 'terminology'); print()
# assert codelist_out.where(f.col('terminology') != 'SNOMED').count() == 0
# NOTE: HW advised that we should not use ICD10 codes for covariates - to agree with other collaborators

print('codelist_hx_out_snomed\n')
codelist_hx_out_snomed = (
  codelist_out
  .where(f.col('terminology') == 'SNOMED')
)
tmpt = tab(codelist_hx_out_snomed, 'name', 'terminology'); print()
print(codelist_hx_out_snomed.orderBy('name', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hx_out = {
  'gdppr': ['gdppr_prepared', 'codelist_hx_out_snomed', 1]
}

# run codelist match and codelist match summary functions
hx_out, hx_out_1st, hx_out_1st_wide = codelist_match(dict_hx_out, _name_prefix=f'cov_hx_out_'); print()
hx_out_summ_name, hx_out_summ_name_code = codelist_match_summ(dict_hx_out, hx_out); print()

# temp save
hx_out_all = hx_out['all']
hx_out_all = temp_save(df=hx_out_all, out_name=f'{proj}_tmp_covariates_hx_out_all'); print()
hx_out_1st = temp_save(df=hx_out_1st, out_name=f'{proj}_tmp_covariates_hx_out_1st'); print()
hx_out_1st_wide = temp_save(df=hx_out_1st_wide, out_name=f'{proj}_tmp_covariates_hx_out_1st_wide'); print()
hx_out_summ_name = temp_save(df=hx_out_summ_name, out_name=f'{proj}_tmp_covariates_hx_out_summ_name'); print()
hx_out_summ_name_code = temp_save(df=hx_out_summ_name_code, out_name=f'{proj}_tmp_covariates_hx_out_summ_name_code'); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

count_var(hx_out_1st_wide, 'PERSON_ID')

# COMMAND ----------

# check result
display(hx_out_1st_wide)

# COMMAND ----------

# check codelist match summary by name and source
display(hx_out_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(hx_out_summ_name_code)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 HX Charlson

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Codelist

# COMMAND ----------

# check
display(codelist_char)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Create

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hx_char = {
  'hes_apc': ['hes_apc_long_prepared', 'codelist_char',  1]
}

# run codelist match and codelist match summary functions
hx_char, hx_char_1st, hx_char_1st_wide = codelist_match(dict_hx_char, _name_prefix=f'cov_hx_char_'); print()
hx_char_summ_name, hx_char_summ_name_code = codelist_match_summ(dict_hx_char, hx_char); print()

# temp save
hx_char_all = hx_char['all']
hx_char_all = temp_save(df=hx_char_all, out_name=f'{proj}_tmp_covariates_hx_char_all'); print()
hx_char_1st = temp_save(df=hx_char_1st, out_name=f'{proj}_tmp_covariates_hx_char_1st'); print()
hx_char_1st_wide = temp_save(df=hx_char_1st_wide, out_name=f'{proj}_tmp_covariates_hx_char_1st_wide'); print()
hx_char_summ_name = temp_save(df=hx_char_summ_name, out_name=f'{proj}_tmp_covariates_hx_char_summ_name'); print()
hx_char_summ_name_code = temp_save(df=hx_char_summ_name_code, out_name=f'{proj}_tmp_covariates_hx_char_summ_name_code'); print()

# COMMAND ----------

# check
display(hx_char_1st_wide)

# COMMAND ----------

# Make a wide table with:
# - One row per patient
# - One column per comorbidity with a 0 or 1 flag indicateing the absence or presence of the comorbidity per patient
cci_df = (
  hx_char_1st_wide
  .drop("CENSOR_DATE_START", "CENSOR_DATE_END")
)

columns = ["mi", "chf", "pad", "cevd", "dementia", "cpd", "rheumd", "pud", "diab", "diabwc", "hp", "rend",
           "mld", "sld", "canc", "metacanc", "hiv"]
assert len(columns) == 17
for index, item in enumerate(columns):
  cci_df = (
    cci_df
    .withColumnRenamed(f'''cov_hx_char_{item}_flag''', item)
    .drop(f'''cov_hx_char_{item}_date''')
    .withColumn(item, f.when(f.col(item).isNull(), f.lit(0)).otherwise(f.col(item)))
  )
  
  # check
  tmpt = tab(cci_df, item); print()

# COMMAND ----------

display(cci_df)

# COMMAND ----------

# Quan CCI calculator
df = cci_df
# Mapping of comorbidities to Quan weights
map_all = cci_weight_dic

# Add columns to store weights
col_list = map_all.keys()
prefix = "cci"
new_col_list = [f'''{prefix}_{x}'''  for x in col_list]
for col, new_col in zip(col_list, new_col_list):
    df = df.withColumn(new_col, f.when(
        f.col(col) == 1, f.lit(map_all.get(col))).otherwise(f.lit(0)))
#Add another set of columns for mutual-exclusive Quan (original Quan)
prefix_mutual_exclusive = "mex"
no_mex_col_list = [x for x in new_col_list if x not in ["cci_diab", "cci_canc", "cci_mld"]]
mex_only_list = ["mex_cci_diab", "mex_cci_canc", "mex_cci_mld"]
# Add columns for mild conditions of mutually-exclusive comorbidity pairs
df = df.withColumn("mex_cci_diab", f.when(f.col("cci_diabwc") != 0, f.lit(
        0)).otherwise(f.col("cci_diab")))
df= df.withColumn("mex_cci_canc", f.when(f.col("cci_metacanc") != 0, f.lit(
        0)).otherwise(f.col("cci_canc")))
df= df.withColumn("mex_cci_mld", f.when(f.col("cci_sld") != 0, f.lit(
        0)).otherwise(f.col("cci_mld")))
mex_col_list= ["mex_cci_diab", "mex_cci_canc", "mex_cci_mld"]
new_col_list_mex = no_mex_col_list+mex_col_list

# Add a list of comorbidities as a column
df = df.withColumn("comorbidity_set", f.array([f.lit("temp")]))
for index, item in enumerate(col_list):
  df = df.withColumn(f'''temp_{item}''', f.when(f.col(item)==1, f.array(f.lit(item))).otherwise(f.array([f.lit("temp")])))
  df = df.withColumn("comorbidity_set", f.array_union(f.col("comorbidity_set"), f.col(f'''temp_{item}''')))
  df = df.drop(f'''temp_{item}''')
  df = df.withColumn("comorbidity_set", f.array_remove(f.col("comorbidity_set"), "temp"))

# Calculate CCI 

# Comment out the next line to have Quan wiht no mutual-exclusivity condition applied
df = df.withColumn("Experimental_quan_all", f.expr('+'.join(new_col_list))) 
df = df.withColumn("cci_index", f.expr('+'.join(new_col_list_mex)))
df_final = df
df_final= df_final.drop(*new_col_list)
df_final = df_final.drop(*new_col_list_mex)
df_final = df_final.drop(*col_list)
df_final = df_final.drop("Experimental_quan_all")
display(df_final)
 
# col_list = original flags indicating the presence of absence of a comorbidity (e.g. mi = 1 measns the patient has myocardial infarction)
# new_col_list = weights of each comorbidity. This set contains weights without taking mutual-exclusivity into account
# mex_col_list = The list of mild conditions of mutualy-exclusive conditions. for example if a patient has both mld and msld, and if we are interested in mutual-exclusive Quan index, 
# then quan_msld=4, mex_quan_mld = 0, and quan_mld=2 (which will be ignored for mutual-exclusivity) 
# new_col_list_mex = The list of columns to be used if mutual-exclusibity is needed.


# COMMAND ----------

# check
tmpt = tab(df_final, 'cci_index'); print()

# COMMAND ----------

hx_char_1st_wide_2 = df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Check

# COMMAND ----------

count_var(hx_char_1st_wide, 'PERSON_ID'); print()
count_var(hx_char_1st_wide_2, 'PERSON_ID'); print()

# COMMAND ----------

# check codelist match summary by name and source
display(hx_char_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(hx_char_summ_name_code)

# COMMAND ----------

# MAGIC %md # F Save

# COMMAND ----------

# merge 
tmp1 = merge(hx_out_1st_wide, hx_char_1st_wide_2, ['PERSON_ID'], validate='1:1', indicator=0); print()
# merge in cohort ID
tmp2 = merge(tmp1, cohort.select('PERSON_ID'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# tmp3 = (
#   hx_out_1st_wide
#   .join(hx_char_1st_wide_2, on=['PERSON_ID'], how='outer')
#   .join(cohort.select('PERSON_ID'), on=['PERSON_ID'], how='outer')
# )

# check
#count_var(tmp3, 'PERSON_ID'); print()
#print(len(tmp3.columns)); print()
#print(pd.DataFrame({f'_cols': tmp3.columns}).to_string()); print()

# COMMAND ----------

# save
save_table(df=tmp2, out_name=f'{proj}_out_covariates', save_previous=True, dbc=dbc)

# repoint
tmp2 = spark.table(f'{dbc}.{proj}_out_covariates')

# COMMAND ----------

# check final
display(tmp2)