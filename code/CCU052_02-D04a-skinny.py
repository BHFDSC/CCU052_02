# Databricks notebook source
# MAGIC %md # CCU052_02-D04a-skinny
# MAGIC
# MAGIC **Description** This notebook creates the key patient characteristics table based on work from CCU002 (previously known as the skinny patient table). Information on age (MYOB), sex, and ethnicity are harmonised and selected from multiple data sources (i.e., GDPPR, HES APC, HES AE, and HES OP). A single record is created for each individual prioritising the most recent non-null non-unknown value. Ethnicity codes are mapped to descriptions and categories.  
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** gdppr, hes_apc, hes_ae, hes_op
# MAGIC
# MAGIC **Data output** tmp_skinny

# COMMAND ----------

# MAGIC %md # 0. Setup 

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

# MAGIC %run "../../../../shds/common/skinny_20250331"

# COMMAND ----------

# MAGIC %md # 1. Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 2. Data

# COMMAND ----------

gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
hes_ae  = extract_batch_from_archive(parameters_df_datasets, 'hes_ae')
hes_op  = extract_batch_from_archive(parameters_df_datasets, 'hes_op')

# COMMAND ----------

# MAGIC %md # 3. Create

# COMMAND ----------

# MAGIC %md ## 3.1. Harmonised (unassembled)

# COMMAND ----------

kpc_harmonised = key_patient_characteristics_harmonise(gdppr=gdppr, hes_apc=hes_apc, hes_ae=hes_ae, hes_op=hes_op)

# temp save (~15 minutes)
outName = f'{proj}_tmp_kpc_harmonised'.lower()
kpc_harmonised.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
kpc_harmonised = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ## 3.2. Selected (assembled)

# COMMAND ----------

kpc_selected = key_patient_characteristics_select(harmonised=kpc_harmonised)

# COMMAND ----------

# MAGIC %md # 4. Save

# COMMAND ----------

# save
save_table(df=kpc_selected, out_name=f'{param_table_tmp_skinny}', save_previous=True, dbc=dbc)

# repoint
kpc_selected = spark.table(f'{dbc}.{param_table_tmp_skinny}')

# COMMAND ----------

# MAGIC %md # 5. Check

# COMMAND ----------

count_var(kpc_selected, 'PERSON_ID')

# COMMAND ----------

display(kpc_selected)

# COMMAND ----------

# see .\data_checks\"skinny" - for further checks