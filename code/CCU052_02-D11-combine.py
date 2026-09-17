# Databricks notebook source
# MAGIC %md # CCU052_02-D11-combine
# MAGIC
# MAGIC **Description** This notebook creates the analysis table, combining the cohort, covariates, and outcomes tables, adding a chunk column for the purpose of reading the table into RStudio or Stata via ODBC.
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** out_cohort, out_covariates, out_outcomes
# MAGIC
# MAGIC **Data output** out_combined

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

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_outcomes')
outcomes = spark.table(f'{dbc}.{proj}_out_outcomes')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

covariates_prepared = (
  covariates
  .withColumnRenamed('CENSOR_DATE_START', 'cov_CENSOR_DATE_START')
  .withColumnRenamed('CENSOR_DATE_END', 'cov_CENSOR_DATE_END')
)

outcomes_prepared = (
  outcomes
  .withColumnRenamed('CENSOR_DATE_START', 'out_CENSOR_DATE_START')
  .withColumnRenamed('CENSOR_DATE_END', 'out_CENSOR_DATE_END')
)

# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------

# tmp1 = merge(cohort, covariates, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()
# tmp2 = merge(tmp1, outcomes, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()

tmp2 = (
  cohort
  .join(covariates_prepared, on='PERSON_ID', how='left')
  .join(outcomes_prepared, on='PERSON_ID', how='left')
)

# add chunk
tmp3 = (
  tmp2
  .withColumn('CHUNK', f.floor(f.rand(seed=1234) * 10) + f.lit(1))
)

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

# check
count_var(tmp3, 'PERSON_ID'); print()
print(len(tmp3.columns)); print()
print(pd.DataFrame({f'_cols': tmp3.columns}).to_string()); print()
tmpt = tab(tmp3, 'CHUNK'); print()

# COMMAND ----------

display(tmp3)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# save
save_table(df=tmp3, out_name=f'{proj}_out_combined', save_previous=True, dbc=dbc)

# repoint
combined = spark.table(f'{dbc}.{proj}_out_combined')

# COMMAND ----------

# MAGIC %md # 6 Check

# COMMAND ----------

# check 
display(combined)

# COMMAND ----------

# check 
tmpt = tab(combined, 'cov_hx_out_asthma_flag'); print()
tmpt = tab(combined, 'cov_hx_out_copd_flag'); print()
tmpt = tab(combined, 'cov_hx_out_ild_flag'); print()

# COMMAND ----------

# check 
tmpt = tab(combined, 'out_asthma_gdppr_flag'); print()
tmpt = tab(combined, 'out_copd_gdppr_flag'); print()
tmpt = tab(combined, 'out_ild_gdppr_flag'); print()

tmpt = tab(combined, 'out_asthma_hes_apc_flag'); print()
tmpt = tab(combined, 'out_copd_hes_apc_flag'); print()
tmpt = tab(combined, 'out_ild_hes_apc_flag'); print()
tmpt = tab(combined, 'out_cvd_hes_apc_flag'); print()

tmpt = tab(combined, 'out_asthma_death_flag'); print()
tmpt = tab(combined, 'out_copd_death_flag'); print()
tmpt = tab(combined, 'out_ild_death_flag'); print()
tmpt = tab(combined, 'out_cvd_death_flag'); print()

# COMMAND ----------

# check prevalence vs incidence
tmp4 = (
  combined
  .na.fill(value=0, subset=['cov_hx_out_asthma_flag', 'cov_hx_out_copd_flag', 'cov_hx_out_ild_flag', 'out_asthma_gdppr_flag', 'out_copd_gdppr_flag', 'out_ild_gdppr_flag'])
  .withColumn('concat_asthma', f.concat(f.col('cov_hx_out_asthma_flag'), f.col('out_asthma_gdppr_flag')))
  .withColumn('concat_copd', f.concat(f.col('cov_hx_out_copd_flag'), f.col('out_copd_gdppr_flag')))
  .withColumn('concat_ild', f.concat(f.col('cov_hx_out_ild_flag'), f.col('out_ild_gdppr_flag'))) 
)
tmpt = tab(tmp4, 'concat_asthma'); print()
tmpt = tab(tmp4, 'concat_copd'); print()
tmpt = tab(tmp4, 'concat_ild'); print()