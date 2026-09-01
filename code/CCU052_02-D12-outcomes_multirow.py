# Databricks notebook source
# MAGIC %md # CCU052_02-D12-outcomes_multirow
# MAGIC
# MAGIC **Description** This notebook creates the multirow outcomes table for the project, that is, not limited to the first outcomes per patient, this table includes multiple rows per patient. A 14-day washout period has been applied to the same type of outcomes. Outcomes are defined with respect to the study start date using the project codelist together with GDPPR, HES APC, and Deaths. 
# MAGIC
# MAGIC **Authors** Thomas Bolton, Hannah Whittaker (based on work from CCU002 and the Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data input** codelist_outcomes_multirow, out_cohort, cur_hes_apc_long, gdppr, hes_apc
# MAGIC
# MAGIC **Data output** out_outcomes_multirow

# COMMAND ----------

spark.sql('CLEAR CACHE')
# spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

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

from pyspark.sql import DataFrame

def washout(df: DataFrame, days: int) -> DataFrame:
  
  # person_id: str, name: str, date: str, code: str, 
  
  '''
  Applies washout period
  
  Args:
    df: Spark DataFrame.
    person_id: NOT CODED YET
    name: NOT CODED YET
    date: NOT CODED YET
    code: NOT CODED YET
    days: washout period (e.g., 30 days)   
    
  Example usage:
    washout(df, days = 30)
    >> 
    
  Notes:
    ...
  '''
  
  print('=================================================================================')
  print('washout')
  print('=================================================================================')  
  
  print('Note: This function currently uses PERSON_ID, name, DATE, CODE')
  print('      arguments will be added at a later date to assign different named variables'); print()
  
  # check columns that we use below are not in df already - as these will be overwritten and dropped
  common_cols = [col for col in df.columns if col in ['_rownum_DATE', '_rownum', '_diff', '_diff_cumsum', '_diff_cumsum_flag']]
  assert len(common_cols) == 0, f'common_cols = {common_cols}'
  
  # define windows to calculate row numbers and date differences 
  # window for duplicate DATE
  _win_DATE = Window\
    .partitionBy('PERSON_ID', 'name', 'DATE')\
    .orderBy('CODE')
  # main window (with stable ordering for duplicate DATE)
  _win = Window\
    .partitionBy('PERSON_ID', 'name')\
    .orderBy('DATE', '_rownum_DATE')

  print('---------------------------------------------------------------------------------')
  print(f'check') 
  print('---------------------------------------------------------------------------------')      
  # check the differences between events before applying a washout period
  
  # calculate row numbers and differences  
  _df_tmp1 = (
    df
    .withColumn('_rownum_DATE', f.row_number().over(_win_DATE))
    .withColumn('_rownum', f.row_number().over(_win))
    .withColumn('_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win)))
  )
  
  # check
  tmpt = tabstat(_df_tmp1, '_diff', byvar='name'); print()  
  
  print('---------------------------------------------------------------------------------')
  print(f'initialise') 
  print('---------------------------------------------------------------------------------')    
  # initialise
  _df_master = []
  _df_washedout = []
  _df_working = df
  _counter_working = _df_working.count()
  print(f'{_counter_working:,} events in total initially'); print()

  # iterate through
  i = 0
  while _counter_working > 0:
    i += 1
    
    print('---------------------------------------------------------------------------------')
    print(f'iteration = {i}') 
    print('---------------------------------------------------------------------------------')
    # calculate row numbers, differences and flag
    _df_working = (
      _df_working
      .withColumn('_rownum_DATE', f.row_number().over(_win_DATE))
      .withColumn('_rownum', f.row_number().over(_win))
      .withColumn('_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win)))
      .withColumn('_diff_cumsum', f.sum(f.col('_diff')).over(_win))
      .withColumn('_diff_cumsum_flag', 
                  f.when(f.col('_rownum') == 1, f.lit('0_first'))
                  .when(f.col('_diff_cumsum') > days, '2_gt_washout')
                  .otherwise('1_le_washout')
                 )
    )

    # calculate maximum row number
    # _rownum_max = _df_working.agg(f.max(f.col('_rownum'))).collect()[0][0]

    # print number of records and maximum row number for this iteration
    # the latter provides an idea of expected number of subsequent iterations
    # print(f'{_counter_working:,} events in total (_rownum_max per person_id and name = {_rownum_max})'); print()

    # tabulate records identified as first events, within the washout, after the washout by name
    ### tmpt = tab(_df_working, 'name', '_diff_cumsum_flag'); print()

    # output first row for this iteration to the master dataframe
    _df_out = (
      _df_working
      .where(f.col('_rownum') == 1)
      .drop('_rownum_DATE', '_rownum', '_diff', '_diff_cumsum', '_diff_cumsum_flag')
    )
    ### _counter_firstevents = _df_out.count()
    
    if(i == 1): 
      _df_master = _df_out
    else:
      _df_master = (
        _df_master
        .unionByName(_df_out)
      )
      
    # cache  
    # _df_master.cache().count()  
    # _df_master = temp_save(df=_df_master, out_name=f'hds_tb_tmp_df_master'); print()
    _df_master = _df_master.localCheckpoint()


    # remove the first row and rows within the washout period from the working dataframe (for the next iteration)
    # remove the first row
    _df_working = (
      _df_working
      .where(f.col('_rownum') != 1)
    )
    ### print(f'{_counter_firstevents:,} events stored for being the first event per person_id and name of iteration {i}')

    # count the rows within the washout period before removing below
    _df_washedout = (
      _df_working
      .where(f.col('_diff_cumsum') <= days)
    )
    ### _counter_washedout = _df_washedout.count()
    ### print(f'{_counter_washedout:,} events excluded for being within {days} days of the first event per person_id and name of iteration {i}'); print()

    # remove the rows within the washout period (i.e., keep rows with a difference greater than the washout)
    _df_working = (
      _df_working\
      .where(f.col('_diff_cumsum') > days)
    )
    _counter_working = _df_working.count()
    
    # cache  
    # _df_working.cache().count()     
    # _df_working = temp_save(df=_df_working, out_name=f'hds_tb_tmp_df_working'); print()
    _df_working = _df_working.localCheckpoint()


  print('---------------------------------------------------------------------------------')
  print(f'check') 
  print('---------------------------------------------------------------------------------')      
  # check that differences between events are now all greater than the washout period
  
  # calculate row numbers, differences  
  _df_tmp2 = (
    _df_master
    .withColumn('_rownum_DATE', f.row_number().over(_win_DATE))
    .withColumn('_rownum', f.row_number().over(_win))
    .withColumn('_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win)))
  )
  
  # check
  tmpt = tabstat(_df_tmp2, '_diff', byvar='name'); print()
  _diff_min = _df_tmp2.agg(f.min(f.col('_diff'))).collect()[0][0]
  assert _diff_min > days, '# # # # something went wrong - differences remain that are still less than the washout period # # # #'  
  print(f'differences between events per person_id and name > {days} days is satisfied')
  
  return _df_master

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_02-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

spark.sql(f'REFRESH TABLE {dbc}.{proj}_codelist_outcomes_multirow')
codelist_out = spark.table(f'{dbc}.{proj}_codelist_outcomes_multirow')

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

spark.sql(f'REFRESH TABLE {dbc}.{param_table_cur_hes_apc_long}')
hes_apc_long = spark.table(f'{dbc}.{param_table_cur_hes_apc_long}')

gdppr = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md ## 2.1 Codelist

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('codelist')
print('--------------------------------------------------------------------------------------')
# check
tmpt = tab(codelist_out, 'name', 'terminology'); print()
print(codelist_out.limit(10).toPandas().to_string()); print()

list_terminology = list(codelist_out.select('terminology').distinct().orderBy('terminology').toPandas()['terminology'])
assert list_terminology == ['ICD10', 'SNOMED']

# edit names
codelist_out = (
  codelist_out
  .where(f.col('name') != 'aeasthma_supporting')
  .withColumn('name', 
              f.when(f.col('terminology') == 'SNOMED', f.concat(f.col('name'), f.lit('_gp')))
              .when(f.col('terminology') == 'ICD10', f.concat(f.col('name'), f.lit('_hes')))
              .otherwise(f.concat(f.col('name'), f.lit('_SHOULD_NOT_BE_HERE')))
             )
)

tmpt = tab(codelist_out, 'name', 'terminology'); print()
print(codelist_out.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 2.2 Cohort

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

# MAGIC %md ## 2.3 GDPPR

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

# temp save (checkpoint)
gdppr_prepared = temp_save(df=gdppr_prepared, out_name=f'{proj}_tmp_outcomes_multirow_gdppr'); print()

# check
count_var(gdppr_prepared, 'PERSON_ID'); print()
tmpt = tabstat(gdppr_prepared, 'DATE', date=1); print()
print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# COMMAND ----------

gdppr_prepared = spark.table(f'{dbc}.{proj}_tmp_outcomes_multirow_gdppr')

# COMMAND ----------

# MAGIC %md ## 2.4 HES APC

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# retain all diagnosis positions (required for chronic outcomes, restrict to primary position for Acute outcomes)
# reduce and rename columns

vlist_diag = [col for col in hes_apc.columns if re.match('^DIAG_[34]_.*$', col)]
print(vlist_diag); print()

hes_apc_prepared = (
  hes_apc
  .select(f.col('PERSON_ID_DEID').alias('PERSON_ID'), f.col('EPISTART').alias('DATE'), *vlist_diag)
)

# check
# count_var(hes_apc_prepared, 'PERSON_ID'); print()

# add individual censor dates
hes_apc_prepared = (
  hes_apc_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
hes_apc_prepared = (
  hes_apc_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# temp save (checkpoint)
hes_apc_prepared = temp_save(df=hes_apc_prepared, out_name=f'{proj}_tmp_outcomes_multirow_hes_apc'); print()

# check
count_var(hes_apc_prepared, 'PERSON_ID'); print()
tmpt = tabstat(hes_apc_prepared, 'DATE', date=1); print()
print(hes_apc_prepared.limit(10).toPandas().to_string()); print()  

# COMMAND ----------

hes_apc_prepared = spark.table(f'{dbc}.{proj}_tmp_outcomes_multirow_hes_apc')

# COMMAND ----------

# MAGIC %md ## 2.5 HES APC long

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc long')
print('--------------------------------------------------------------------------------------')
# retain all diagnosis positions (required for chronic outcomes, restrict to primary position for Acute outcomes)
# reduce and rename columns
hes_apc_long_prepared = (
  hes_apc_long
  .select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()
# tmpt = tab(hes_apc_long_prepared, 'DIAG_POSITION'); print()

# add individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )
)

# temp save (checkpoint)
hes_apc_long_prepared = temp_save(df=hes_apc_long_prepared, out_name=f'{proj}_tmp_outcomes_multirow_hes_apc_long'); print()

# check
count_var(hes_apc_long_prepared, 'PERSON_ID'); print()
tmpt = tabstat(hes_apc_long_prepared, 'DATE', date=1); print()
print(hes_apc_long_prepared.limit(10).toPandas().to_string()); print()  

# COMMAND ----------

hes_apc_long_prepared = spark.table(f'{dbc}.{proj}_tmp_outcomes_multirow_hes_apc_long') #.drop('DIAG_POSITION', 'DIAG_DIGITS')

# COMMAND ----------

# MAGIC %md # 3 Codelist match

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# check
tmpt = tab(codelist_out, 'name', 'terminology'); print()
print(codelist_out.limit(10).toPandas().to_string()); print()

# SNOMED only
codelist_out_SNOMED = (
  codelist_out
  .where(f.col('terminology') == 'SNOMED')
)

# check
tmpt = tab(codelist_out_SNOMED, 'name', 'terminology'); print()
print(codelist_out_SNOMED.limit(10).toPandas().to_string()); print()

# ICD10 only
codelist_out_ICD10 = (
  codelist_out
  .where(f.col('terminology') == 'ICD10')
  .where(~f.col('name').isin(['aecopd_hes', 'aeasthma_hes'])) # handled separately below due to additional logic requiring wide table 
)

# check
tmpt = tab(codelist_out_ICD10, 'name', 'terminology'); print()
print(codelist_out_ICD10.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

# - ILD: ICD10 in first diagnostic coding position only 

# check only aeild_hes
assert codelist_out_ICD10.select('name').distinct().collect()[0][0] == 'aeild_hes'

# check
tmpt = tab(hes_apc_long_prepared, 'DIAG_POSITION')

hes_apc_long_prepared_1 = (
  hes_apc_long_prepared
  .where(f.col('DIAG_POSITION') == 1)
)

# check
tmpt = tab(hes_apc_long_prepared_1, 'DIAG_POSITION')

# tidy
hes_apc_long_prepared_1 = hes_apc_long_prepared_1.drop('DIAG_POSITION', 'DIAG_DIGITS')

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_out = {
  'gdppr':  ['gdppr_prepared',  'codelist_out_SNOMED', 1]
  , 'hes_apc':  ['hes_apc_long_prepared_1',  'codelist_out_ICD10', 1]
}

# run codelist match and codelist match summary functions
out = codelist_match_stages_to_run(dict_out, _name_prefix=f'out_', stages_to_run = 1); print()
out_summ_name, out_summ_name_code = codelist_match_summ(dict_out, out); print()

# temp save
out_all = out['all']
out_all = temp_save(df=out_all, out_name=f'{proj}_tmp_outcomes_multirow_out_all'); print()
# out_1st = temp_save(df=out_1st, out_name=f'{proj}_tmp_outcomes_out_1st'); print()
# out_1st_wide = temp_save(df=out_1st_wide, out_name=f'{proj}_tmp_outcomes_out_1st_wide'); print()
out_summ_name = temp_save(df=out_summ_name, out_name=f'{proj}_tmp_outcomes_multirow_out_summ_name'); print()
out_summ_name_code = temp_save(df=out_summ_name_code, out_name=f'{proj}_tmp_outcomes_multirow_out_summ_name_code'); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

# check
tmpt = tab(out_all, 'name'); print()
count_var(out_all, 'PERSON_ID'); print()

# COMMAND ----------

# check result
display(out_all.orderBy('PERSON_ID', 'DATE', 'CODE'))

# COMMAND ----------

# check codelist match summary by name and source
display(out_summ_name.orderBy('name'))

# COMMAND ----------

# check codelist match summary by name, source, and code
display(out_summ_name_code.orderBy('name', 'code'))

# COMMAND ----------

# MAGIC %md # 4 Bespoke - aecopd_hes

# COMMAND ----------

# MAGIC %md ## 4.1 Create

# COMMAND ----------

# aecopd_rule_1 : J441 - any position
# aecopd_rule_2 : J440 - any position
# aecopd_rule_3 : J449 - first position 
# aecopd_rule_4 : J22  - first position and only if J448 in second position

aecopd_hes = (
  hes_apc_prepared
  .withColumn('aecopd_rule_1', f.when(f.col('DIAG_4_CONCAT').rlike('J441'), 1).otherwise(0))
  .withColumn('aecopd_rule_2', f.when(f.col('DIAG_4_CONCAT').rlike('J440'), 1).otherwise(0))
  .withColumn('aecopd_rule_3', f.when(f.col('DIAG_4_01').rlike('J449'), 1).otherwise(0))
  .withColumn('aecopd_rule_4', f.when((f.col('DIAG_4_01').rlike('J22')) & (f.col('DIAG_4_02').rlike('J448')), 1).otherwise(0))
  .withColumn('aecopd_hes_max', f.greatest(f.col('aecopd_rule_1'), f.col('aecopd_rule_2'), f.col('aecopd_rule_3'), f.col('aecopd_rule_4')))
  .withColumn('aecopd_hes_concat', f.concat(f.col('aecopd_rule_1'), f.col('aecopd_rule_2'), f.col('aecopd_rule_3'), f.col('aecopd_rule_4')))
)

# COMMAND ----------

# MAGIC %md ## 4.2 Check

# COMMAND ----------

# checks
tmpt = tab(aecopd_hes, 'aecopd_rule_1'); print()
tmpt = tab(aecopd_hes, 'aecopd_rule_2'); print()
tmpt = tab(aecopd_hes, 'aecopd_rule_3'); print()
tmpt = tab(aecopd_hes, 'aecopd_rule_4'); print()

count_var(aecopd_hes.where(f.col('aecopd_rule_1') == 1), 'PERSON_ID'); print()
count_var(aecopd_hes.where(f.col('aecopd_rule_2') == 1), 'PERSON_ID'); print()
count_var(aecopd_hes.where(f.col('aecopd_rule_3') == 1), 'PERSON_ID'); print()
count_var(aecopd_hes.where(f.col('aecopd_rule_4') == 1), 'PERSON_ID'); print()

tmpt = tab(aecopd_hes, 'aecopd_hes_concat', 'aecopd_hes_max'); print()

display(aecopd_hes)

# COMMAND ----------

# MAGIC %md ## 4.3 Reformat

# COMMAND ----------

out_aecopd_hes = (
  aecopd_hes
  .where(f.col('aecopd_hes_max') == 1)
  .select('PERSON_ID', 'DATE')
  .withColumn('name', f.lit('aecopd_hes'))
)

# check
count_var(out_aecopd_hes, 'PERSON_ID'); print()
tmpt = tab(out_aecopd_hes, 'name'); print()
print(out_aecopd_hes.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 5 Bespoke - aeasthma_hes

# COMMAND ----------

# MAGIC %md ## 5.1 Create

# COMMAND ----------

# check aeasthma ICD-10 codelist remains J45 (inc J450, J451, J458, J459) and J46
list_aeasthma_hes = list(
  codelist_out
  .where(f.col('name') == 'aeasthma_hes')
  .select('CODE')
  .orderBy('CODE')
  .toPandas()['CODE']
)
assert list_aeasthma_hes == ['J45', 'J450', 'J451', 'J458', 'J459', 'J46']

# aeasthma_hes_rule_1 : Any age          
#           & aeasthma ICD-10 codelist in first position 
# aeasthma_hes_rule_2 : Aged 5 and under 
#           & R062 in first position 
#           & aeasthma ICD-10 codelist in second position
# aeasthma_hes_rule_3 : Aged 5 and under 
#           & B349 in first position 
#           & R062 in second position 
#           & aeasthma ICD-10 codelist in third position

aeasthma_hes = (
  merge(hes_apc_prepared, cohort.select('PERSON_ID', 'DOB'), ['PERSON_ID'], validate='m:1', assert_results=['both', 'right_only'], keep_results=['both'], indicator=0)  
  .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25)  
  .withColumn('aeasthma_hes_rule_1', f.when(f.col('DIAG_4_01').rlike('J45|J46'), 1).otherwise(0))
  .withColumn('aeasthma_hes_rule_2', 
              f.when(
                (f.col('age') < 6) # 5 and under
                & (f.col('DIAG_4_01').rlike('R062')) 
                & (f.col('DIAG_4_02').rlike('J45|J46'))
                , 1)
              .otherwise(0)
              )
  .withColumn('aeasthma_hes_rule_3', 
              f.when(
                (f.col('age') < 6) # 5 and under
                & (f.col('DIAG_4_01').rlike('B349')) 
                & (f.col('DIAG_4_02').rlike('R062'))
                & (f.col('DIAG_4_03').rlike('J45|J46'))
                , 1)
              .otherwise(0)
              )  
  .withColumn('aeasthma_hes_rule_max', f.greatest(f.col('aeasthma_hes_rule_1'), f.col('aeasthma_hes_rule_2'), f.col('aeasthma_hes_rule_3')))
  .withColumn('aeasthma_hes_rule_concat', f.concat(f.col('aeasthma_hes_rule_1'), f.col('aeasthma_hes_rule_2'), f.col('aeasthma_hes_rule_3')))  
)
    

# COMMAND ----------

# MAGIC %md ## 5.2 Check

# COMMAND ----------

# checks
tmpt = tab(aeasthma_hes, 'aeasthma_hes_rule_1'); print()
tmpt = tab(aeasthma_hes, 'aeasthma_hes_rule_2'); print()
tmpt = tab(aeasthma_hes, 'aeasthma_hes_rule_3'); print()

count_var(aeasthma_hes.where(f.col('aeasthma_hes_rule_1') == 1), 'PERSON_ID'); print()
count_var(aeasthma_hes.where(f.col('aeasthma_hes_rule_2') == 1), 'PERSON_ID'); print()
count_var(aeasthma_hes.where(f.col('aeasthma_hes_rule_3') == 1), 'PERSON_ID'); print()

tmpt = tab(aeasthma_hes, 'aeasthma_hes_rule_concat', 'aeasthma_hes_rule_max'); print()
tmpt = tabstat(aeasthma_hes, 'age'); print()
tmpt = tabstat(aeasthma_hes.where(f.col('aeasthma_hes_rule_1') == 1), 'age'); print()
tmpt = tabstat(aeasthma_hes.where(f.col('aeasthma_hes_rule_2') == 1), 'age'); print()
tmpt = tabstat(aeasthma_hes.where(f.col('aeasthma_hes_rule_3') == 1), 'age'); print()

display(aeasthma_hes)

# COMMAND ----------

# MAGIC %md ## 5.3 Reformat

# COMMAND ----------

out_aeasthma_hes = (
  aeasthma_hes
  .where(f.col('aeasthma_hes_rule_max') == 1)
  .select('PERSON_ID', 'DATE')
  .withColumn('name', f.lit('aeasthma_hes'))
)

# check
count_var(out_aeasthma_hes, 'PERSON_ID'); print()
tmpt = tab(out_aeasthma_hes, 'name'); print()
print(out_aeasthma_hes.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 6 Bespoke - aeild_hes

# COMMAND ----------

#

# COMMAND ----------

# MAGIC %md # 6 Prepare

# COMMAND ----------

# MAGIC %md ## 6.1 Combine

# COMMAND ----------

# check result
print(out_all.limit(10).toPandas().to_string()); print()
print(out_aecopd_hes.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()
print(out_aeasthma_hes.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()
tmpt = tab(out_all, 'name'); print()
tmpt = tab(out_aecopd_hes, 'name'); print()
tmpt = tab(out_aeasthma_hes, 'name'); print()

# select
out_all_select = (
  out_all
  .select('PERSON_ID', 'DATE', 'name')
)

# check
print(out_all_select.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# union
tmpw1 = (
  out_all_select
  .unionByName(out_aecopd_hes)
  .unionByName(out_aeasthma_hes)
)

# check
tmpt = tab(tmpw1, 'name'); print()
print(tmpw1.orderBy('PERSON_ID', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md ## 6.2 Remove duplicates

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('remove duplicate PERSON_ID, name, DATE')
print('---------------------------------------------------------------------------------')
# check
count_varlist(tmpw1, ['PERSON_ID', 'name', 'DATE'])

# add row number
win_rownum = Window\
  .partitionBy('PERSON_ID', 'name', 'DATE')\
  .orderBy('CODE')
win_rownummax = Window\
  .partitionBy('PERSON_ID', 'name', 'DATE')
tmpw1 = (
  tmpw1
  .withColumn('CODE', f.lit(1))
  .withColumn('rownum', f.row_number().over(win_rownum))
  .withColumn('rownummax', f.count(f.lit(1)).over(win_rownummax))
)  

# check
tmpt = tab(tmpw1, 'rownum'); print()
tmpt = tab(tmpw1, 'name', 'rownum', var2_wide=0); print()
# tmpt = tab(tmpq1.where(f.col('rownum') == 1), 'rownummax'); print()

# filter
tmpw2 = (
  tmpw1
  .where(f.col('rownum') == 1)
)

# check
tmpt = tab(tmpw2, 'rownum'); print()
count_varlist(tmpw2, ['PERSON_ID', 'name', 'DATE'])

# tidy
tmpw2 = tmpw2.drop('rownum', 'rownummax')

# check
print(tmpw2.orderBy('PERSON_ID', 'name', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 7 Washout

# COMMAND ----------

# MAGIC %md ## 7.1 Save

# COMMAND ----------

tmpw2 = temp_save(df=tmpw2, out_name=f'{proj}_tmp_outcomes_multirow_tmpw2'); print() 

# COMMAND ----------

# MAGIC %md ## 7.2 Check

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print(f'check') 
print('---------------------------------------------------------------------------------')      
# check the differences between events

# add row numbers, differences 
_win_ord = Window\
  .partitionBy('PERSON_ID', 'name')\
  .orderBy('DATE')
_win = Window\
  .partitionBy('PERSON_ID', 'name')
tmpw2 = (
  tmpw2
  .withColumn('_rownum', f.row_number().over(_win_ord))
  .withColumn('_rownummax', f.count(f.lit(1)).over(_win))
  .withColumn('_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win_ord)))
)

# check
tmpt = tab(tmpw2.where(f.col('_rownum') == 1), '_rownummax'); print()
tmpt = tab(tmpw2.where(f.col('_rownum') == 1), '_rownummax', 'name'); print()
tmpt = tabstat(tmpw2, '_diff', byvar='name'); print()
# 

# tidy
# tmpw2 = tmpw2.drop('_rownum', '_rownummax', '_diff')

# check
print(tmpw2.orderBy('PERSON_ID', 'name', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# 

# COMMAND ----------

# MAGIC %md ## 7.3 Algorithm

# COMMAND ----------

tmpw2 = spark.table(f'{dbc}.{proj}_tmp_outcomes_multirow_tmpw2')


# COMMAND ----------

# check
display(tmpw2)

# tmpw2.cache().count()
# print(tmpw2.rdd.getNumPartitions())
# tmpw2 = tmpw2.repartition(1).localCheckpoint()
# print(tmpw2.rdd.getNumPartitions())
# spark.sql("set spark.sql.shuffle.partitions=1")

# localCheckpoint() - 7 mins - tmpw3
# cache() - 2 hours - tmpw3a
# compared files - same

# COMMAND ----------

tmpw3 = washout(tmpw2, days=14)

# COMMAND ----------

# temp save
# tmpw3 = temp_save(df=tmpw3, out_name=f'{proj}_tmp_outcomes_multirow_tmpw3'); print()

# temp save
# tmpw3 = temp_save(df=tmpw3, out_name=f'{proj}_tmp_outcomes_multirow_tmpw3a'); print()

# temp save
tmpw3 = temp_save(df=tmpw3, out_name=f'{proj}_tmp_outcomes_multirow_tmpw3b'); print()

# COMMAND ----------

# tmpw2a = tmpw2.where(f.col('_rownummax') <= 50).drop('_rownum', '_rownummax', '_diff')
# tmpw2b = tmpw2.where(f.col('_rownummax') >  50).drop('_rownum', '_rownummax', '_diff')

# COMMAND ----------

# spark.sql("set spark.sql.shuffle.partitions=10")
# print(sqlContext.getConf("spark.sql.shuffle.partitions"))
# tmpw3a = washout(tmpw2a, days=14)

# # temp save
# tmpw3a = temp_save(df=tmpw3a, out_name=f'{proj}_tmp_outcomes_multirow_tmpw3a'); print()

# spark.sql("set spark.sql.shuffle.partitions=10")
# print(sqlContext.getConf("spark.sql.shuffle.partitions"))
# tmpw3b = washout(tmpw2b, days=14)

# # temp save
# tmpw3b = temp_save(df=tmpw3b, out_name=f'{proj}_tmp_outcomes_multirow_tmpw3b'); print() 

# # check
# tmpw3ax = tmpw3a.select('PERSON_ID', 'name').distinct()
# tmpw3bx = tmpw3b.select('PERSON_ID', 'name').distinct()
# tmpf = merge(tmpw3ax, tmpw3bx, ['PERSON_ID', 'name'], validate='1:1', assert_results=['left_only', 'right_only'])

# # combine
# tmpw3 = (
#   tmpw3a
#   .unionByName(tmpw3b)
# )

# count_varlist(tmpw3, ['PERSON_ID', 'name'])

# COMMAND ----------

# MAGIC %md ## 7.4 Check

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print(f'check') 
print('---------------------------------------------------------------------------------')      
# check that differences between events are now all greater than the washout period

# add row numbers, differences 
_win_ord = Window\
  .partitionBy('PERSON_ID', 'name')\
  .orderBy('DATE')
_win = Window\
  .partitionBy('PERSON_ID', 'name')
tmpw3 = (
  tmpw3
  .withColumn('_rownum', f.row_number().over(_win_ord))
  .withColumn('_rownummax', f.count(f.lit(1)).over(_win))
  .withColumn('_diff', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win_ord)))
)

# check
tmpt = tab(tmpw3.where(f.col('_rownum') == 1), '_rownummax'); print()
tmpt = tab(tmpw3.where(f.col('_rownum') == 1), '_rownummax', 'name'); print()
tmpt = tabstat(tmpw3, '_diff', byvar='name'); print()

# tidy
tmpw3 = tmpw3.drop('_rownum', '_rownummax', '_diff')

# check
print(tmpw3.orderBy('PERSON_ID', 'name', 'DATE').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# check an individual with a large number of events
_win = Window\
  .partitionBy('PERSON_ID', 'name')\
  .orderBy('DATE')

tmpq2 = tmpw2\
  .where(f.col('PERSON_ID') == '')\
  .withColumn('_diff_pre_washout', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win)))

tmpq3 = tmpw3\
  .where(f.col('PERSON_ID') == '')\
  .withColumn('_diff_post_washout', f.datediff(f.col('DATE'), f.lag(f.col('DATE'), 1).over(_win)))

tmpf = (
  merge(tmpq2, tmpq3, 
        ['CODE',
         'PERSON_ID',
         'DATE',
         #'CENSOR_DATE_START',
         #'CENSOR_DATE_END',
         'name'
         #,'source',
         #'sourcen'
      ], validate='1:1', assert_results=['both', 'left_only'], indicator=1)
)

display(tmpf.orderBy('PERSON_ID', 'name', 'DATE'))

# check
tmpq2 = hes_apc_prepared\
  .where(f.col('PERSON_ID') == '')
display(tmpq2.orderBy('DATE'))

tmpq2 = out_all\
  .where(f.col('PERSON_ID') == '')
display(tmpq2.orderBy('DATE', 'CODE'))

# COMMAND ----------

# check
count_var(tmpw3, 'PERSON_ID'); print()
count_varlist(tmpw3, ['PERSON_ID', 'name', 'DATE'])
print(len(tmpw3.columns)); print()
print(pd.DataFrame({f'_cols': tmpw3.columns}).to_string()); print()

# events
tmpt = tab(tmpw3, 'name'); print() # 'source'

# individuals
_win_ord = (
  Window
  .partitionBy('PERSON_ID', 'name') # 'source'
  .orderBy('DATE')
)
tmpf = (
  tmpw3
  .withColumn('_rownum', f.row_number().over(_win_ord))
)
tmpt = tab(tmpf.where(f.col('_rownum') == 1), 'name'); print() # 'source'

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

# save
save_table(df=tmpw3, out_name=f'{proj}_out_outcomes_multirow', save_previous=True, dbc=dbc)

# repoint
tmpw3 = spark.table(f'{dbc}.{proj}_out_outcomes_multirow')

# COMMAND ----------

# MAGIC %md # A Check

# COMMAND ----------

count_var(tmpw3, 'PERSON_ID'); print()
count_varlist(tmpw3, ['PERSON_ID', 'name', 'DATE'])

# COMMAND ----------

# MAGIC %md ## A.1 Display

# COMMAND ----------

# check
display(tmpw3.orderBy('PERSON_ID', 'name', 'DATE'))

# COMMAND ----------

# MAGIC %md ## A.2 Numerical summaries of plots

# COMMAND ----------

# prepare
_tmp = merge(tmpw3, cohort.select('PERSON_ID', 'DOB', 'SEX'), ['PERSON_ID'], validate='m:1', keep_results=['both', 'left_only'], indicator=1); print()
# assert_results=['both', 'right_only'],  # as above
 
_tmp = (
  _tmp  
  .withColumn('source', f.lit(1))
  .withColumn('fu', f.datediff(f.col('DATE'), f.to_date(f.lit('2019-11-01')))/365.25) # f.col('CENSOR_DATE_END'))/365.25)
  .withColumn('age', f.datediff(f.col('DATE'), f.col('DOB'))/365.25)
  .select('PERSON_ID', 'name', 'source', 'DATE', 'fu', 'age', 'SEX', '_merge')
)

# COMMAND ----------

# check numerical summaries 
_tmps = (
  _tmp  
)
tmpt = tabstat(_tmps, 'fu', byvar=['name', 'source']); print()
tmpt = tabstat(_tmps, 'DATE', byvar=['name', 'source'], date=1); print()
tmpt = tabstat(_tmps, 'age',  byvar=['name', 'source']); print()
tmpt = tabstat(_tmps, 'age',  byvar=['name', 'sex']); print()

# COMMAND ----------

# plot prepare
_tmpp = _tmp.toPandas()
# _tmpp['DATE'] = pd.to_datetime(_tmpp['DATE']).dt.date

# COMMAND ----------

# plot function
# row_height was 2.4
def check_dist(df, var, bin_min, bin_max, sharey, stacked, xlabel, byvar='source', row_height=4.8, out_com='out'):
  
  # plot parameters
  plt.rcParams.update({'font.size': 8})
  rows_of_5 = np.ceil(len(df['name'].drop_duplicates())/5).astype(int)
  fig, axes = plt.subplots(rows_of_5, 4, figsize=(13,row_height*rows_of_5), sharex=True, sharey=sharey) # , sharey=True , dpi=100) # 
  
  
  # lists
  name_list = list(df[['name']].drop_duplicates().sort_values('name')['name']) 
  print(f'name_list = {name_list}')
  source_list = list(df[['source']].drop_duplicates().sort_values('source')['source']) 
  print(f'source_list = {source_list}')
  colors = sns.color_palette("tab10", 2) # len(source_list))
  
  print(f'var = {var}')
  

  # min and max dates
  if(var == 'DATE'):
    _min = min(df[f'{var}'])
    _max = max(df[f'{var}'])
    print(_min, _max)  
  
  # loop over names
  for i, (ax, v) in enumerate(zip(axes.flatten(), name_list)):
    print('  ', i, ax, v)
    
    # filter
    tmp2d1 = df[(df[f'name'] == v)] # (_tmpp[f'{var}'] > -20) &     
    
    if(byvar!='source'):
      names = ['Male', 'Female']  
      s1 = list(tmp2d1[tmp2d1[f'{byvar}'] == '1'][f'{var}'])
      s2 = list(tmp2d1[tmp2d1[f'{byvar}'] == '2'][f'{var}'])
      ax.hist(s1, bins = list(np.linspace(bin_min, bin_max, 100)), color=colors[0], label=names[0], alpha=0.5) # normed=True 
      ax.hist(s2, bins = list(np.linspace(bin_min, bin_max, 100)), color=colors[1], label=names[1], alpha=0.5) # normed=True 
    else:
      # names = ['hes_apc', 'gdppr']  
      
      
      
      if((bin_min == 0) & (bin_max == 0)): bins = 100
      else: bins = list(np.linspace(bin_min, bin_max, 100))

      
      if(var == 'DATE'):
        tt1 = np.linspace(pd.Timestamp(_min).value, pd.Timestamp(_max).value, 100)
        tt2 = pd.to_datetime(tt1)
        tt3 = mdates.date2num(tt2)
        bins = list(tt3)
        print(bins)      
        
        # ax.hist([s1, s2], bins = bins, stacked=stacked, color=colors, label=names) # normed=True      
#         ax.hist([tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'{var}']], bins = bins, color=colors[0], label=names[0], alpha=0.5) # normed=True 
#         ax.hist([tmp2d1[tmp2d1[f'source'] == 'gdppr'][f'{var}']], bins = bins, color=colors[1], label=names[1], alpha=0.5) # normed=True 
        for j, source in enumerate(source_list):
          print('    ', j, source)          
          ax.hist([tmp2d1[tmp2d1[f'source'] == source][f'{var}']], bins = bins, color=colors[j], label=source, alpha=0.5) # normed=True 
      else:
        # ax.hist([s1, s2], bins = bins, stacked=stacked, color=colors, label=names) # normed=True      
        for j, source in enumerate(source_list):
          print('    ', j, source)          
          s1 = list(tmp2d1[tmp2d1[f'source'] == source][f'{var}'])
          ax.hist([s1], bins = bins, color=colors[j], label=source, alpha=0.5) # normed=True 
        

    # plot reformats  
    ax.set_title(f'{v}')
    ax.set(xlabel=f'{xlabel}')
    ax.xaxis.set_tick_params(labelbottom=True)    
    if(var == 'DATE'): 
      ax.xaxis.set_tick_params(rotation=90) # labelbottom=True)    
      ax.set(xlim=(_min, _max))  
    if(i==0): ax.legend(loc='upper left')
       
#     if(out_com=='out'):
#       axes[2,4].set_axis_off()
#       for i in range(0,2):
#         for j in range(0, 5):
#           axes[i,j].xaxis.set_tick_params(labelbottom=True)
#     elif(out_com=='com'):      
#       axes[7,3].set_axis_off()
#       axes[7,4].set_axis_off()
#       for i in range(0,7):
#         for j in range(0, 5):
#           axes[i,j].xaxis.set_tick_params(labelbottom=True)          
  plt.tight_layout();
  return fig

# COMMAND ----------

# MAGIC %md ## A.3 Check distributions over follow-up time (years) by data source

# COMMAND ----------

fig1 = check_dist(df=_tmpp, var='fu', bin_min=0, bin_max=3.2, sharey=False, stacked=False, xlabel='\nFollow-up (years)\n')
display(fig1)

# COMMAND ----------

fig2 = check_dist(df=_tmpp, var='fu', bin_min=0, bin_max=3.2, sharey=True, stacked=False, xlabel='\nFollow-up (years)\n')
display(fig2)

# COMMAND ----------

# MAGIC %md ## A.4 Check distributions over calendar time by data source

# COMMAND ----------

# row_height was 2.8
fig3 = check_dist(df=_tmpp, var='DATE', row_height=5.2, bin_min=0, bin_max=0, sharey=False, stacked=False, xlabel='\nDate\n')
display(fig3)

# COMMAND ----------

# fig4 = plot_hx_1st(df=_tmpp, var='DATE', row_height=2.8, bin_min=0, bin_max=0, sharey=True, stacked=True, xlabel='\nDate\n')
# display(fig4)

# COMMAND ----------

# MAGIC %md ## A.5 Check distributions over age at event (years) by data source

# COMMAND ----------

fig5 = check_dist(df=_tmpp, var='age', bin_min=0, bin_max=115, sharey=False, stacked=False, xlabel='\nAge (years)\n')
display(fig5)

# COMMAND ----------

# fig6 = plot_hx_1st(df=_tmpp, var='age', bin_min=0, bin_max=20, sharey=True, stacked=True, xlabel='\nAge (years)\n')
# display(fig6)

# COMMAND ----------

# MAGIC %md ## A.6 Check distributions over age at event (years) by sex

# COMMAND ----------

fig7 = check_dist(df=_tmpp, var='age', byvar='SEX', bin_min=0, bin_max=115, sharey=False, stacked=False, xlabel='\nAge (years)\n')
display(fig7)

# COMMAND ----------

# fig8 = plot_hx_1st(df=_tmpp, var='age', byvar='SEX', bin_min=0, bin_max=20, sharey=True, stacked=True, xlabel='\nAge (years)\n')
# display(fig8)

# COMMAND ----------

# MAGIC %md ## A.7 Codelist match summaries