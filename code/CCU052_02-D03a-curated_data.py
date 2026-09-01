
# Databricks notebook source
# MAGIC %md # CCU052_02-D03a-curated_data
# MAGIC
# MAGIC **Description** This notebook creates the curated tables, which includes long format HES APC diagnostic table, one-record per individual deaths table, long format cause of death table.
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** hes_apc, deaths
# MAGIC
# MAGIC **Data output** cur_hes_apc_long, cur_deaths_nodup, cur_deaths_nodup_long
# MAGIC

# COMMAND ----------

# MAGIC %md # 0. Setup

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

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')
gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

# COMMAND ----------

# MAGIC %md # 3. HES_APC

# COMMAND ----------

# MAGIC %md ## 3.1 Check

# COMMAND ----------

# check
count_var(hes_apc, 'PERSON_ID_DEID'); print()
count_var(hes_apc, 'EPIKEY'); print()

# check for null EPISTART and potential use ADMIDATE to supplement
tmpp = (
  hes_apc
  .select('EPISTART', 'ADMIDATE')
  .withColumn('_EPISTART', f.when(f.col('EPISTART').isNotNull(), 1).otherwise(0))
  .withColumn('_ADMIDATE', f.when(f.col('ADMIDATE').isNotNull(), 1).otherwise(0))
)
tmpt = tab(tmpp, '_EPISTART', '_ADMIDATE'); print()
# => ADMIDATE is always null when EPISTART is null

# COMMAND ----------

# MAGIC %md ## 3.2 Diag long

# COMMAND ----------

# MAGIC %md ### 3.2.1 Create

# COMMAND ----------

# select columns (PERSON_ID, RECORD_ID, DATE, Diagnostic columns)
# rename PERSON_ID
hes_apc_prepared = (
  hes_apc  
  .select(['PERSON_ID_DEID', 'EPIKEY', 'EPISTART'] 
          + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
  .orderBy('PERSON_ID', 'EPIKEY')
)

# check
display(hes_apc_prepared)

# COMMAND ----------

# reshape twice, tidy, and remove records with missing code

# reshape
hes_apc_long = (
  reshape_wide_to_long_multi(hes_apc_prepared, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
  .withColumn('_tmp', f.substring(f.col('DIAG_4_'), 1, 3))
  .withColumn('_chk', udf_null_safe_equality('DIAG_3_', '_tmp').cast(t.IntegerType()))
  .withColumn('_DIAG_4_len', f.length(f.col('DIAG_4_')))
  .withColumn('_chk2', f.when((f.col('_DIAG_4_len').isNull()) | (f.col('_DIAG_4_len') <= 4), 1).otherwise(0))
)

# check
tmpt = tab(hes_apc_long, '_chk'); print()
assert hes_apc_long.where(f.col('_chk') == 0).count() == 0
tmpt = tab(hes_apc_long, '_DIAG_4_len'); print()
tmpt = tab(hes_apc_long, '_chk2'); print()
assert hes_apc_long.where(f.col('_chk2') == 0).count() == 0

# tidy
hes_apc_long = (
  hes_apc_long
  .drop('_tmp', '_chk')
)

# reshape
hes_apc_long = reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

# MAGIC %md ### 3.2.2 Save

# COMMAND ----------

# save before checks for efficiency
outName = f'{param_table_cur_hes_apc_long}'.lower()  
print(outName)
hes_apc_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
hes_apc_long = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ### 3.2.3 Check

# COMMAND ----------

# check
count_var(hes_apc_long, 'PERSON_ID'); print()
count_var(hes_apc_long, 'EPIKEY'); print()

# check removal of trailing X
tmpp = hes_apc_long\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpp, 'flag'); print()
tmpt = tab(tmpp.where(f.col('CODE').rlike('X')), 'CODE', 'flag', var2_unstyled=1); print()

# COMMAND ----------

# check
display(hes_apc_long)

# COMMAND ----------

# MAGIC %md # 3. Deaths

# COMMAND ----------

# MAGIC %md ### 3.1. Check

# COMMAND ----------

# check
count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID'); print()
assert dict(deaths.dtypes)['REG_DATE'] == 'string'
assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# COMMAND ----------

# MAGIC %md ## 3.2. No duplicate

# COMMAND ----------

# MAGIC %md ### 3.2.1. Create

# COMMAND ----------

# setting recommended by the data wranglers to avoid the following error message:
# SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] 
# You may get a different result due to the upgrading to Spark >= 3.0:
# Caused by: DateTimeParseException: Text '2009029' could not be parsed at index 6
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# define window for the purpose of creating a row number below as per the skinny patient table
win_personid_ord = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))
win_personid = Window\
  .partitionBy('PERSON_ID')

# select columns required
# rename ID
# remove records with missing IDs
# reformat dates
deaths_tmp1 = (
  deaths
  .select([f.col('DEC_CONF_NHS_NUMBER_CLEAN_DEID').alias('PERSON_ID'), 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])
  .where(f.col('PERSON_ID').isNotNull())
  .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))
  .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))
  .withColumn('rownum', f.row_number().over(win_personid_ord))
  .withColumn('rownummax', f.count(f.lit(1)).over(win_personid))
)

# check
tmpt = tab(deaths_tmp1.where(f.col('rownum') == 1), 'rownummax')

# reduce to a single row per individual as per the skinny patient table
# rename column ahead of reshape below
# sort by ID
deaths_nodup = (
  deaths_tmp1  
  .where(f.col('rownum') == 1)
  .drop('rownum', 'rownummax')
  .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')
  .orderBy('PERSON_ID')
)

# COMMAND ----------

# check
display(deaths_tmp1.where(f.col('rownummax') > 1).orderBy('PERSON_ID', 'rownum'))

# COMMAND ----------

tmpf = deaths_tmp1.where(f.col('rownummax') > 1)
count_var(tmpf, 'PERSON_ID'); print()
count_varlist(tmpf, ['PERSON_ID', 'REG_DATE_OF_DEATH'])

# COMMAND ----------

# MAGIC %md ### 3.2.2. Save

# COMMAND ----------

outName = f'{proj}_cur_deaths_nodup'.lower()
deaths_nodup.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
deaths_nodup = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ### 3.2.3. Check

# COMMAND ----------

# check
count_var(deaths_nodup, 'PERSON_ID'); print()
count_var(deaths_nodup, 'REG_DATE_OF_DEATH'); print()
count_var(deaths_nodup, 'S_COD_CODE_UNDERLYING'); print()

# COMMAND ----------

# check
display(deaths_nodup)

# COMMAND ----------

# MAGIC %md ## 3.3. COD long 

# COMMAND ----------

# Cause of Death (COD) long

# COMMAND ----------

# MAGIC %md ### 3.3.1. Create

# COMMAND ----------

# remove records with missing DOD
deaths_tmp2 = (
  deaths_nodup
  .where(f.col('REG_DATE_OF_DEATH').isNotNull())
  .drop('REG_DATE')
)

# check
count_var(deaths_tmp2, 'PERSON_ID'); print()
count_var(deaths_tmp2, 'REG_DATE_OF_DEATH'); print()

# reshape
# add 1 to diagnosis position to start at 1 (c.f., 0) - will avoid confusion with HES long, which start at 1
# rename 
# remove records with missing cause of death
deaths_nodup_long = reshape_wide_to_long(deaths_tmp2, i=['PERSON_ID', 'REG_DATE_OF_DEATH'], j='DIAG_POSITION', stubname='S_COD_CODE_')\
  .withColumn('DIAG_POSITION', f.when(f.col('DIAG_POSITION') != 'UNDERLYING', f.concat(f.lit('SECONDARY_'), f.col('DIAG_POSITION'))).otherwise(f.col('DIAG_POSITION')))\
  .withColumnRenamed('S_COD_CODE_', 'CODE4')\
  .where(f.col('CODE4').isNotNull())\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')\
  .withColumn('CODE3', f.substring(f.col('CODE4'), 1, 3))
deaths_nodup_long = reshape_wide_to_long(deaths_nodup_long, i=['PERSON_ID', 'DATE', 'DIAG_POSITION'], j='DIAG_DIGITS', stubname='CODE')\
  .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))

# COMMAND ----------

# MAGIC %md ### 3.3.2. Save

# COMMAND ----------

outName = f'{proj}_cur_deaths_nodup_long'.lower()
deaths_nodup_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
deaths_nodup_long = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ### 3.3.3. Check

# COMMAND ----------

# check
count_var(deaths_nodup_long, 'PERSON_ID'); print()  
tmpt = tab(deaths_nodup_long, 'DIAG_POSITION', 'DIAG_DIGITS'); print() 
tmpt = tab(deaths_nodup_long, 'CODE', 'DIAG_DIGITS'); print()   
# TODO - add valid ICD-10 code checker...

# COMMAND ----------

# check
display(deaths_nodup_long)

# COMMAND ----------

# MAGIC %md # 4 GDPPR

# COMMAND ----------

# MAGIC %md ## Create

# COMMAND ----------

# gdppr_id = (
#   gdppr
#   .withColumn('mono_inc_id', f.monotonically_increasing_id())
# )

# COMMAND ----------

# MAGIC %md ## Check

# COMMAND ----------

# check
# display(gdppr_id)

# COMMAND ----------

# tmpt = tabstat(gdppr_id, 'mono_inc_id')

# COMMAND ----------

# MAGIC %md ## Save

# COMMAND ----------

# # save
# outName = f'{proj}_cur_gdppr'.lower()
# gdppr_id.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')