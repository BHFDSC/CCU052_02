# Databricks notebook source
# MAGIC %md # CCU052_02-D09-cohort_flags
# MAGIC
# MAGIC **Description** This notebook creates the flags for the asthma, COPD and ILD cohorts, and examines the overlap between these. 
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** out_cohort, out_covariates
# MAGIC
# MAGIC **Data output** out_cohort_flags

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

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

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_covariates')
covariates = spark.table(f'{dbc}.{proj}_out_covariates')

# COMMAND ----------

# check
count_var(cohort, 'PERSON_ID'); print()
count_var(covariates, 'PERSON_ID'); print()

# check
display(cohort)
display(covariates)

# COMMAND ----------

# check
tmpt = tabstat(covariates, 'cov_hx_out_asthma_date', byvar='cov_hx_out_asthma_flag', date=1); print()
tmpt = tabstat(covariates, 'cov_hx_out_copd_date', byvar='cov_hx_out_copd_flag', date=1); print()
tmpt = tabstat(covariates, 'cov_hx_out_ild_date', byvar='cov_hx_out_ild_flag', date=1); print()

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# reduce
cohort_tmp = (
  cohort
  .select('PERSON_ID', 'DOB')
)
covariates_tmp = (
  covariates
  .select('PERSON_ID', 'cov_hx_out_asthma_date', 'cov_hx_out_copd_date', 'cov_hx_out_ild_date', 'cov_hx_out_asthma_flag', 'cov_hx_out_copd_flag', 'cov_hx_out_ild_flag')
)

# merge
tmp1 = merge(cohort_tmp, covariates_tmp, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()

# create age at first event
tmp1 = (
  tmp1
  .withColumn('cov_hx_out_asthma_age', f.round((f.datediff(f.col('cov_hx_out_asthma_date'), f.col('DOB')))/365.25, 1))
  .withColumn('cov_hx_out_copd_age', f.round((f.datediff(f.col('cov_hx_out_copd_date'), f.col('DOB')))/365.25, 1))
  .withColumn('cov_hx_out_ild_age', f.round((f.datediff(f.col('cov_hx_out_ild_date'), f.col('DOB')))/365.25, 1))
)

# check
tmpt = tabstat(tmp1, 'cov_hx_out_asthma_age'); print()
tmpt = tabstat(tmp1, 'cov_hx_out_copd_age'); print()
tmpt = tabstat(tmp1, 'cov_hx_out_ild_age'); print()

# COMMAND ----------

# check
display(tmp1.where(f.col('cov_hx_out_asthma_date').isNotNull()).orderBy('PERSON_ID'))

# COMMAND ----------

# MAGIC %md # 3 Check

# COMMAND ----------

# asthma
tmpp = (
  tmp1
  .withColumn('age', f.round(f.col('cov_hx_out_asthma_age')*5)/5)
  .groupBy('age')
  .agg(f.count(f.lit(1)).alias('n'))
  .toPandas()
)

plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['age'], tmpp['n'], width = 1/5, edgecolor = None)
axes.set(xticks=np.arange(0, 116, step=5))
axes.set_xlim(0,116)
axes.set(xlabel="Age (years)")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
# display(fig)

# COMMAND ----------

# copd
tmpp = (
  tmp1
  .withColumn('age', f.round(f.col('cov_hx_out_copd_age')*5)/5)
  .groupBy('age')
  .agg(f.count(f.lit(1)).alias('n'))
  .toPandas()
)

plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['age'], tmpp['n'], width = 1/5, edgecolor = None)
axes.set(xticks=np.arange(0, 116, step=5))
axes.set_xlim(0,116)
axes.set(xlabel="Age (years)")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
# display(fig)

# COMMAND ----------

# ild
tmpp = (
  tmp1
  .withColumn('age', f.round(f.col('cov_hx_out_ild_age')*5)/5)
  .groupBy('age')
  .agg(f.count(f.lit(1)).alias('n'))
  .toPandas()
)

plt.rcParams.update({'font.size': 8})
fig, axes = plt.subplots(1, 1, figsize=(15,5), sharex=False) # was 4.75 for 2
axes.bar(tmpp['age'], tmpp['n'], width = 1/5, edgecolor = None)
axes.set(xticks=np.arange(0, 116, step=5))
axes.set_xlim(0,116)
axes.set(xlabel="Age (years)")
axes.set(ylabel="Number of individuals")
axes.spines['right'].set_visible(False)
axes.spines['top'].set_visible(False)
# display(fig)

# COMMAND ----------

# MAGIC %md # 4 Flags

# COMMAND ----------

tmp1 = (
  tmp1
  .withColumn('cohort_flag_asthma', f.when(f.col('cov_hx_out_asthma_flag') == 1, 1).otherwise(0))
  .withColumn('cohort_flag_copd',   f.when((f.col('cov_hx_out_copd_flag') == 1) & (f.col('cov_hx_out_copd_age') >= 40), 1).otherwise(0))
  .withColumn('cohort_flag_ild',    f.when((f.col('cov_hx_out_ild_flag') == 1)  & (f.col('cov_hx_out_ild_age')  >= 40), 1).otherwise(0))
)

# check
tmpt = tab(tmp1, 'cov_hx_out_asthma_flag', 'cohort_flag_asthma'); print()

tmpt = tab(tmp1, 'cov_hx_out_copd_flag', 'cohort_flag_copd'); print()
tmpt = tab(tmp1.where(f.col('cov_hx_out_copd_flag') == 1), 'cohort_flag_copd'); print()
tmpt = tabstat(tmp1.where(f.col('cov_hx_out_copd_flag') == 1), 'cov_hx_out_copd_age', byvar='cohort_flag_copd'); print()

tmpt = tab(tmp1, 'cov_hx_out_ild_flag', 'cohort_flag_ild'); print()
tmpt = tab(tmp1.where(f.col('cov_hx_out_ild_flag') == 1), 'cohort_flag_ild'); print()  
tmpt = tabstat(tmp1.where(f.col('cov_hx_out_ild_flag') == 1), 'cov_hx_out_ild_age', byvar='cohort_flag_ild'); print()

# COMMAND ----------

# check overlap
tmp1 = (
  tmp1
  .withColumn('concat', f.concat(f.col('cohort_flag_asthma'), f.col('cohort_flag_copd'), f.col('cohort_flag_ild')))
)
tmpt = tab(tmp1, 'concat'); print()
tmpt = tab(tmp1.where(f.col('cohort_flag_asthma') == 1), 'concat'); print()
tmpt = tab(tmp1.where(f.col('cohort_flag_copd') == 1), 'concat'); print()
tmpt = tab(tmp1.where(f.col('cohort_flag_ild') == 1), 'concat'); print()

# COMMAND ----------

# check
display(tmp1)

# COMMAND ----------

# MAGIC %md # 5 Prepare

# COMMAND ----------

tmp2 = (
  tmp1
  .select('PERSON_ID', 'cohort_flag_asthma', 'cohort_flag_copd', 'cohort_flag_ild')
)

# check 
count_var(tmp2, 'PERSON_ID'); print()
tmpt = tab(tmp2, 'cohort_flag_asthma'); print()
tmpt = tab(tmp2, 'cohort_flag_copd'); print()
tmpt = tab(tmp2, 'cohort_flag_ild'); print()

# COMMAND ----------

# MAGIC %md # 6 Save

# COMMAND ----------

# save
save_table(df=tmp2, out_name=f'{proj}_out_cohort_flags', save_previous=True, dbc=dbc)

# repoint
tmp2 = spark.table(f'{dbc}.{proj}_out_cohort_flags')