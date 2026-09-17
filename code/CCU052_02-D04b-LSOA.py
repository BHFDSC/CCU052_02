# Databricks notebook source
# MAGIC %md # CCU052_02-D04b-LSOA
# MAGIC
# MAGIC **Description** This notebook creates the key patient characteristic table for LSOA based on work from CCU002. The LSOA closest to the study start date is extracted for each individual using information contained in the earliest and most recent monthly batch of GDPPR. LSOA codes are mapped to region and the index of multiple deprivation (IMD).   
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** gdppr, gdppr_1st, cur_lsoa_region, cur_lsoa_imd
# MAGIC
# MAGIC **Data output** tmp_lsoa

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Setup

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

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

lsoa_region = spark.table(f'{dbc}.{param_table_cur_lsoa_region}')
lsoa_imd    = spark.table(f'{dbc}.{param_table_cur_lsoa_imd}')

gdppr = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

archived_on_1st = '2020-11-23'
gdppr_1st = (
  spark.table(f'{dbc_old}.gdppr_{db}_archive')
  .where(f.col('archived_on') == archived_on_1st)
)
print(f'{dbc_old}.gdppr_{db}_archive (archived_on = {archived_on_1st})')
print(f'  {gdppr_1st.count():,} records')

# COMMAND ----------

# MAGIC %md # 3. Prepare

# COMMAND ----------

# get distinct LSOA from the batch of GDPPR
# keep practice for inspection of non-distinct LSOA by PERSON_ID and REPORTING_PERIOD_END_DATE

# use first batch of GDPPR to identify LSOA at nearer time-point to study start date for as much of cohort as possible
# use latest batch of GDPPR to identify LSOA for remainder of cohort
# in future we will use consolidated_lsoa table which pulls patient LSOA histories from multiple sources (under development)

# 1st batch of GDPPR
tmp1 = (
  gdppr_1st
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'PRACTICE', f.col('REPORTING_PERIOD_END_DATE').alias('RPED'), 'LSOA')
  .where(f.col('PERSON_ID').isNotNull())
  .where(f.col('RPED').isNotNull())
  .where(f.col('LSOA').isNotNull())
  .dropDuplicates(['PERSON_ID', 'RPED', 'LSOA'])
  .withColumn('LSOA_batch', f.lit('First'))
)

# latest batch
tmp2 = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'PRACTICE', f.col('REPORTING_PERIOD_END_DATE').alias('RPED'), 'LSOA')
  .where(f.col('PERSON_ID').isNotNull())
  .where(f.col('RPED').isNotNull())
  .where(f.col('LSOA').isNotNull())
  .dropDuplicates(['PERSON_ID', 'RPED', 'LSOA'])
  .withColumn('LSOA_batch', f.lit('Latest'))
)

# union
tmp3 = (
  tmp1
  .union(tmp2)
  .dropDuplicates(['PERSON_ID', 'RPED', 'LSOA'])
)

# temp save (checkpoint)
out_name = f'{proj}_tmp_lsoa_tmp3'
tmp3.write.mode('overwrite').saveAsTable(f'{dbc}.{out_name}')
tmp3 = spark.table(f'{dbc}.{out_name}')

# check
count_var(tmp3, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 4. Filter

# COMMAND ----------

# Filter to the earliest record(s)
# which is the closest to our baseline of 1 Nov 2019
# this will include multiple rows per individual where there are more than one LSOA relating to the earliest date
# we keep these for now and see if they actually result in conflicts for region and IMD
# e.g., an individual may have two different LSOA's on the same date, but these both may have region == London and IMD == 1, so are not conflicting
# on the otherhand if an individual had two different LSOA's on the same date, but these related to different regions and IMD then we have a conflict and should not use region or IMD for this individual
win_denserank = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('RPED')      
win_rownum = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('RPED', 'LSOA')
win_rownummax = Window\
  .partitionBy('PERSON_ID')
tmp4 = (
  tmp3
  .withColumn('dense_rank', f.dense_rank().over(win_denserank))
  .where(f.col('dense_rank') == 1)
  .drop('dense_rank')
  .withColumn('rownum', f.row_number().over(win_rownum))
  .withColumn('rownummax', f.count('PERSON_ID').over(win_rownummax))
  .withColumn('LSOA_1', f.substring(f.col('LSOA'), 1, 1))
)

# check
count_var(tmp4, 'PERSON_ID'); print()
tmpt = tab(tmp4.where(f.col('rownum') == 1), 'rownummax'); print()
tmpt = tab(tmp4, 'LSOA_1'); print()

# COMMAND ----------

# MAGIC %md # 5. Add region

# COMMAND ----------

# prepare mapping
lsoa_region_1 = (
  lsoa_region
  .select(f.col('lsoa_code').alias('LSOA'), 'lsoa_name', f.col('region_name').alias('region'))
)

# check
tmpt = tab(lsoa_region_1, 'region'); print()

# map
tmp5 = merge(tmp4, lsoa_region_1, ['LSOA'], validate='m:1', keep_results=['both', 'left_only']); print()
        
# check
count_var(tmp5, 'PERSON_ID'); print()
tmpt = tab(tmp5, 'LSOA_1', '_merge'); print()

# edit
tmp5 = (
  tmp5
  .withColumn('region',
              f.when(f.col('LSOA_1') == 'W', 'Wales')
              .when(f.col('LSOA_1') == 'S', 'Scotland')
              .otherwise(f.col('region'))
             )
  .drop('_merge')
)

# check
tmpt = tab(tmp5, 'region'); print()

# COMMAND ----------

# MAGIC %md # 6. Add IMD

# COMMAND ----------

# check
tmpt = tab(lsoa_imd, 'IMD_2019_DECILES', 'IMD_2019_QUINTILES'); print()

# map
tmp6 = merge(tmp5, lsoa_imd, ['LSOA'], validate='m:1', keep_results=['both', 'left_only']); print()
  
# check
count_var(tmp6, 'PERSON_ID'); print()
tmpt = tab(tmp6, 'LSOA_1', '_merge', var2_unstyled=1); print()  

# tidy
tmp6 = (
  tmp6
  .drop('_merge')    
)

# temp save (checkpoint)
out_name = f'{proj}_tmp_lsoa_tmp6'
tmp6.write.mode('overwrite').saveAsTable(f'{dbc}.{out_name}')
tmp6 = spark.table(f'{dbc}.{out_name}')

# COMMAND ----------

# MAGIC %md # 7. Collapse

# COMMAND ----------

# collapse to 1 row per individual
win_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
win_lag = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('rownum')

# note - intentionally not using null safe equality for region_lag1_diff
tmp7 = (
  tmp6
  .orderBy('PERSON_ID', 'rownum')
  
  .withColumn('LSOA_conflict', f.when(f.col('rownummax') > 1, 1).otherwise(0))
  
  .withColumn('IMD_min', f.min(f.col('IMD_2019_DECILES')).over(win_egen))
  .withColumn('IMD_max', f.max(f.col('IMD_2019_DECILES')).over(win_egen))
  .withColumn('IMD_conflict', f.lit(1) - udf_null_safe_equality('IMD_min', 'IMD_max').cast(t.IntegerType()))

  .withColumn('region_lag1', f.lag(f.col('region'), 1).over(win_lag))
  .withColumn('region_lag1_diff', f.when(f.col('region') != f.col('region_lag1'), 1).otherwise(0)) 
  .withColumn('region_conflict', f.max(f.col('region_lag1_diff')).over(win_egen))
)

# check
tmpt = tab(tmp7, 'LSOA_conflict', 'IMD_conflict'); print()
tmpt = tab(tmp7, 'LSOA_conflict', 'region_conflict'); print()
tmpt = tab(tmp7, 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp7, 'rownummax', 'region_conflict'); print()
tmpt = tab(tmp7.where(f.col('rownum') == 1), 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp7.where(f.col('rownum') == 1), 'rownummax', 'region_conflict'); print()

# COMMAND ----------

# check
display(tmp7.where(f.col('rownummax') > 1))

# COMMAND ----------

# finalise
tmp8 = (
  tmp7          
  .withColumn('IMD_2019_DECILES', f.when((f.col('rownummax') > 1) & (f.col('IMD_conflict') == 1), f.lit(None)).otherwise(f.col('IMD_2019_DECILES')))       
  .withColumn('region', f.when((f.col('rownummax') > 1) & (f.col('region_conflict') == 1), f.lit(None)).otherwise(f.col('region')))       
  .select('PERSON_ID', 'LSOA_batch', 'LSOA_conflict', 'LSOA', 'lsoa_name', f.col('RPED').alias('LSOA_date'),  'region_conflict', 'region', 'IMD_conflict', 'IMD_2019_DECILES', 'rownum', 'rownummax')
  .where(f.col('rownum') == 1)
)
        
# check
tmpt = tab(tmp8, 'LSOA_batch'); print()
tmpt = tab(tmp8, 'rownummax', 'LSOA_conflict'); print()
tmpt = tab(tmp8, 'rownummax', 'region_conflict'); print()
tmpt = tab(tmp8, 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp8, 'region', 'region_conflict'); print()
tmpt = tab(tmp8, 'IMD_2019_DECILES', 'IMD_conflict'); print()
tmpt = tab(tmp8, 'region_conflict', 'IMD_conflict'); print()
tmpt = tab(tmp8.where(f.col('rownummax') > 1), 'region_conflict', 'IMD_conflict'); print()
tmpt = tabstat(tmp8, 'LSOA_date', date=1); print()

# tidy
tmp9 = (
  tmp8
  .drop('rownum', 'rownummax')
)

# COMMAND ----------

# check
display(tmp9)

# COMMAND ----------

# MAGIC %md # 8. Save

# COMMAND ----------

# save
save_table(df=tmp9, out_name=f'{proj}_tmp_lsoa', save_previous=True, dbc=dbc)